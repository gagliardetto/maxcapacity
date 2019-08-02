package maxcapacity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/facebookgo/httpcontrol"
	"github.com/gagliardetto/futures"
	"github.com/gagliardetto/request"
	"github.com/gagliardetto/roundrobin"
	. "github.com/gagliardetto/utils"
	"github.com/miekg/dns"
	"go.uber.org/ratelimit"
)

func New(host string, port uint) *MaxCapacity {
	// TODO: check destination host
	mxc := &MaxCapacity{
		host: host,
	}
	mxc.init()
	return mxc
}

type MaxCapacity struct {
	host    string
	port    uint
	clients roundrobin.RoundRobin
}

func (mxc *MaxCapacity) init() {
	wait := futures.New()
	key := "updated"
	go func() {
		for {
			var httpClients []interface{}

			///
			ARecordsRes, err := sendDNSRequest(defaultDNS, mxc.host, dns.TypeA)
			if err != nil {
				panic(err)
			}
			aRecords := extractA(ARecordsRes.Answer)
			if len(aRecords) == 0 {
				fmt.Println("error while getting DNS records: no records returned")
				time.Sleep(time.Second)
				continue
			}
			///
			// TODO: update periodically at expiry of dns records:
			for _, ip := range aRecords {
				// TODO: check for destination port (80 or 443), cleanup
				destination := ip.A.String() + ":" + strconv.Itoa(int(mxc.port))
				req := request.NewRequest(NewHTTPClientWithLoadBalancer(mxc.host+":"+strconv.Itoa(int(mxc.port)), destination))
				//req := request.NewRequest(NewHTTPClient())
				req.Headers = map[string]string{
					"Connection": "keep-alive",
				}
				rrrr := &Request2Limit{
					Req:  req,
					host: destination,
					RL:   ratelimit.New(8, ratelimit.WithoutSlack),
					mu:   &sync.RWMutex{},
				}
				httpClients = append(httpClients, rrrr)
				fmt.Println(ip)
			}

			if mxc.clients == nil {
				mxc.clients, err = roundrobin.New(httpClients)
				if err != nil {
					panic(err)
				}
			} else {
				mxc.clients.Replace(httpClients)
			}
			wait.Answer(key, "val", nil)
			var sleepTime uint32
			sleepTime = aRecords[0].Header().Ttl
			for _, ip := range aRecords {
				if ip.Header().Ttl < sleepTime {
					sleepTime = ip.Header().Ttl
				}
			}
			sleepDuration := time.Second * time.Duration(sleepTime-60)
			fmt.Println("next DNS A records update in:", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}()
	wait.Ask(key)
}

var (
	dnsClient = dns.Client{
		DialTimeout: time.Second * 10,
	}
	defaultDNS = getRootResolver()
)

func sendDNSRequest(server string, domain string, t uint16) (*dns.Msg, error) {
	m := new(dns.Msg)

	m.SetQuestion(dns.Fqdn(domain), t)

	// VERY IMPORTANT:
	// NOTE: recursion does not work in Italy (via VPN); the result returns directly the A and CNAME records even if asking the root servers.
	// NOTE: works in NL (via VPN).
	m.RecursionDesired = true

	var dnsReply *dns.Msg
	errs := RetryExponentialBackoff(5, time.Millisecond*500, func() error {
		var err error
		dnsReply, _, err = dnsClient.Exchange(m, defaultDNS+":"+"53")
		return err
	})
	if errs != nil {
		return nil, fmt.Errorf("errors while exchanging DNS message:\n%s", FormatErrorArray("    ", errs))
	}
	return dnsReply, nil
}

func getRootResolver() string {
	config, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		panic(err)
	}
	_ = config
	rootResolver := config.Servers[0]

	return rootResolver
}
func extractA(rr []dns.RR) []*dns.A {
	var Arecords []*dns.A
	for i := range rr {
		a := rr[i]
		if _, ok := a.(*dns.A); ok {
			Arecords = append(Arecords, a.(*dns.A))
		}
	}

	sort.Slice(Arecords, func(i, j int) bool {
		return bytes.Compare(Arecords[i].A, Arecords[j].A) < 0
	})
	return Arecords
}

var (
	DefaultMaxIdleConnsPerHost               = 2000
	DefaultTimeout             time.Duration = 15 * time.Second
	DefaultKeepAlive           time.Duration = 180 * time.Second
)

// NewHTTPClientWithLoadBalancer returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClientWithLoadBalancer(oldAddress, newAddress string) *http.Client {

	tr := &httpcontrol.Transport{
		RetryAfterTimeout:   true,
		RequestTimeout:      DefaultTimeout,
		MaxTries:            3,
		MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
		Proxy:               http.ProxyFromEnvironment,
		Dial:                NewDialer(oldAddress, newAddress),
		DialKeepAlive:       DefaultKeepAlive,
		//TLSClientConfig: &tls.Config{
		//	InsecureSkipVerify: conf.InsecureSkipVerify,
		//},
	}

	return &http.Client{
		Timeout:   DefaultTimeout,
		Transport: tr,
	}
}

var dialer = &net.Dialer{
	Timeout:   DefaultTimeout,
	KeepAlive: DefaultKeepAlive,
	DualStack: true,
}

func NewDialer(oldAddress, newAddress string) func(network, address string) (net.Conn, error) {
	return func(network, address string) (net.Conn, error) {
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			//fmt.Println("address original =", addr)
			if addr == oldAddress {
				addr = newAddress
				fmt.Println("connected to =>", addr)
			}
			return dialer.DialContext(ctx, network, addr)
		}(context.Background(), network, address)
	}
}

type Request2Limit struct {
	Req       *request.Request
	RL        ratelimit.Limiter
	host      string
	mu        *sync.RWMutex
	isWaiting bool
}

func (mxc *MaxCapacity) IsWaiting() bool {
	var hasAtLeastOneFree bool
	mxc.clients.IterateAll(func(i interface{}) bool {
		cl := i.(*Request2Limit)
		if !cl.IsWaiting() {
			hasAtLeastOneFree = true
			return false
		}
		return true
	})
	return !hasAtLeastOneFree
}

///////////////////
func (r2l *Request2Limit) IsWaiting() bool {
	r2l.mu.RLock()
	defer r2l.mu.RUnlock()
	return r2l.isWaiting
}

func (r2l *Request2Limit) SetAsWaiting() {
	r2l.mu.Lock()
	r2l.isWaiting = true
	r2l.mu.Unlock()
}
func (r2l *Request2Limit) SetAsNotWaiting() {
	r2l.mu.Lock()
	r2l.isWaiting = false
	r2l.mu.Unlock()
}

///////////////////
func (mxc *MaxCapacity) GetOnce(url interface{}) (resp *request.Response, err error) {

	// get a client:
	rrrr := mxc.clients.Next().(*Request2Limit)
	if rrrr.IsWaiting() {
		for {
			if rrrr.IsWaiting() {
				time.Sleep(time.Second)
			} else {
				break
			}
		}
	}

	// take a rate token from the client:
	rrrr.RL.Take()

	// try sending the request:
	resp, err = rrrr.Req.Get(url)
	if err != nil {

	} else {
		// here the logic states that we wait only in case of a status 429 (other APIs might have a different status code or logic)
		if resp.StatusCode == http.StatusTooManyRequests {
			rrrr.SetAsWaiting()
		} else {
			rrrr.SetAsNotWaiting()
		}
	}
	return
}
func (mxc *MaxCapacity) GetWithRetry(url interface{}) (resp *request.Response, err error) {
	// get a client:
	rrrr := mxc.clients.Next().(*Request2Limit)
	if rrrr.IsWaiting() {
		for {
			if rrrr.IsWaiting() {
				time.Sleep(time.Second)
			} else {
				break
			}
		}
	}

	for {

		// take a rate token from the client:
		rrrr.RL.Take()

		// try sending the request:
		resp, err = rrrr.Req.Get(url)
		if err != nil {

		} else {
			// here the logic states that we wait only in case of a status 429 (other APIs might have a different status code or logic)
			if resp.StatusCode == http.StatusTooManyRequests {
				fmt.Println(resp.Status, rrrr.host)
				rrrr.SetAsWaiting()

				// read all body and close it to be able to reuse the connection:
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()

				// sleep the ban time:
				time.Sleep(time.Minute)
				continue
			} else {
				fmt.Println(resp.Status, rrrr.host)
				rrrr.SetAsNotWaiting()
				return
			}
		}
	}
	return
}
