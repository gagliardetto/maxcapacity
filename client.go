package maxcapacity

import (
	"bytes"
	"context"
	"errors"
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

// New creates a new instance of the maxcapacity roundrobin multiplexer;
// NOTE: the source and destination ports must be the same
// (i.e. if I want host api.example.com:443, all requests will be direct
// to the host A records like: 0.0.0.0:443, i.e. to the same port as the host)
func New(
	host string,
	port uint,
	rps int,
) (*MaxCapacity, error) {
	// TODO: check destination host
	mxc := &MaxCapacity{
		sourceHost: host,
		port:       port,
		rps:        rps,
	}
	err := mxc.init()
	if err != nil {
		return nil, err
	}
	return mxc, nil
}

type MaxCapacity struct {
	sourceHost string
	port       uint
	clients    roundrobin.RoundRobin
	rps        int
}

// init initializes the loop that will periodically (just before expiry) update
// the A records of the destination domain (to not direct requests to wrong IPs)
func (mxc *MaxCapacity) init() error {
	wait := futures.New()
	key := "updated"
	go func() {
		for {

			///
			// sleep a small time to avoid arriving here when the expiry of records is
			// still zero seconds(getting this to loop a lot because it will sleep zero seconds)
			time.Sleep(time.Second)
			ARecordsRes, err := sendDNSRequest(defaultDNS, mxc.sourceHost, dns.TypeA)
			if err != nil {
				panic(err)
			}
			aRecords := extractA(ARecordsRes.Answer)
			if len(aRecords) == 0 {
				e := errors.New("error while getting DNS records: no records returned")
				fmt.Println(e)
				wait.Answer(key, nil, e)
				time.Sleep(time.Second)
				continue
			}

			///

			var latestDestinations []string
			for _, ip := range aRecords {
				// TODO: check for destination port (80 or 443), cleanup
				destination := ip.A.String() + ":" + strconv.Itoa(int(mxc.port))
				latestDestinations = append(latestDestinations, destination)
			}
			var relevantHTTPClients []interface{}
			var previousDestinations []string
			if mxc.clients != nil {
				mxc.clients.IterateAll(func(i interface{}) bool {
					cl := i.(*Request2Limit)
					previousDestinations = append(previousDestinations, cl.destinationAddress)

					isStillRelevant := SliceContains(latestDestinations, cl.destinationAddress)
					if isStillRelevant {
						relevantHTTPClients = append(relevantHTTPClients, cl)
					}
					return true
				})
			}
			///
			// TODO: update periodically at expiry of dns records:
			for _, ip := range aRecords {
				// TODO: check for destination port (80 or 443), cleanup
				destination := ip.A.String() + ":" + strconv.Itoa(int(mxc.port))

				if !SliceContains(previousDestinations, destination) {
					rrrr := &Request2Limit{
						HTTPClient:         NewHTTPClientWithLoadBalancer(mxc.sourceHost+":"+strconv.Itoa(int(mxc.port)), destination),
						destinationAddress: destination,
						RL:                 ratelimit.New(mxc.rps, ratelimit.WithoutSlack),
						mu:                 &sync.RWMutex{},
					}
					relevantHTTPClients = append(relevantHTTPClients, rrrr)
					fmt.Println("new ip:", ip)
				}

			}

			if mxc.clients == nil {
				mxc.clients, err = roundrobin.New(relevantHTTPClients)
				if err != nil {
					panic(err)
				}
			} else {
				mxc.clients.Replace(relevantHTTPClients)
			}
			wait.Answer(key, "val", nil)
			var sleepTime uint32
			sleepTime = aRecords[0].Header().Ttl
			for _, ip := range aRecords {
				if ip.Header().Ttl < sleepTime {
					sleepTime = ip.Header().Ttl
				}
			}
			sleepDuration := time.Second * time.Duration(sleepTime)
			fmt.Println("next DNS A records update in:", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}()
	_, err := wait.Ask(key)
	return err
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
	MaxIdleConnsPerHost               = 100
	Timeout             time.Duration = 15 * time.Second
	KeepAlive           time.Duration = 15 * time.Second
)

// NewHTTPClientWithLoadBalancer returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClientWithLoadBalancer(oldAddress, newAddress string) *http.Client {

	tr := &httpcontrol.Transport{
		RetryAfterTimeout:   false,
		RequestTimeout:      Timeout,
		MaxTries:            3,
		MaxIdleConnsPerHost: MaxIdleConnsPerHost,
		Proxy:               http.ProxyFromEnvironment,
		Dial:                NewDialer(oldAddress, newAddress),
		DialKeepAlive:       KeepAlive,
		//TLSClientConfig: &tls.Config{
		//	InsecureSkipVerify: conf.InsecureSkipVerify,
		//},
	}

	return &http.Client{
		Timeout:   Timeout,
		Transport: tr,
	}
}

var dialer = &net.Dialer{
	Timeout:   Timeout,
	KeepAlive: KeepAlive,
	DualStack: true,
}

// NewDialer creates a dialer that will redirect connections to a certain address to another specific
// address (connecting to the latter when dialing for a connection instead of the former).
func NewDialer(oldAddress, newAddress string) func(network, address string) (net.Conn, error) {
	return func(network, address string) (net.Conn, error) {
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			//fmt.Println("address original =", addr)
			if addr == oldAddress {
				addr = newAddress
				fmt.Println("conn.", oldAddress, "=>", addr)
			}
			return dialer.DialContext(ctx, network, addr)
		}(context.Background(), network, address)
	}
}

type Request2Limit struct {
	HTTPClient         *http.Client
	RL                 ratelimit.Limiter
	destinationAddress string
	mu                 *sync.RWMutex
	isWaiting          bool
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

func (mxc *MaxCapacity) GetNextR2L() *Request2Limit {
	// get a client:
	return mxc.clients.Next().(*Request2Limit)
}

func (mxc *MaxCapacity) Get(url interface{}, modifier func(req *request.Request) *request.Request) (resp *request.Response, err error) {
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
		// create new request:
		req := request.NewRequest(rrrr.HTTPClient)
		// apply any user-specified changes:
		req = modifier(req)

		// take a rate:
		rrrr.RL.Take()
		// try sending the request:
		resp, err = req.Get(url)
		if err != nil {
			return
		} else {
			// here the logic states that we wait only in case of a status 429 (other APIs might have a different status code or logic)
			if resp.StatusCode == http.StatusTooManyRequests {
				fmt.Println(resp.Status, rrrr.destinationAddress)
				rrrr.SetAsWaiting()

				// read all body and close it to be able to reuse the connection:
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()

				// sleep the ban time:
				time.Sleep(time.Minute)
				continue
			} else {
				fmt.Println(resp.Status, rrrr.destinationAddress)
				rrrr.SetAsNotWaiting()
				return
			}
		}
	}
	return
}
