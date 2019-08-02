package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/maxcapacity"
	"github.com/panjf2000/ants"
)

var (
	start = time.Now()
)

func myFunc(cl *maxcapacity.MaxCapacity, i interface{}) {
	index := i.(string)

	for {

		// try sending the request:
		// NOTE: the host must be the same that was used to create the maxcapacity object.
		resp, err := cl.GetWithRetry("https://api.example.com")
		if err != nil {
			// if there is an error (most likely only client timeout while waiting for headers)
			spew.Dump(err)
		} else {
			// here the bad situation can be only status "429 Too Many Requests";
			// in that case:
			// - stop adding new items to the queue
			// - the retry loop in all other failed in-flight requests is aware of other requests status:
			//   - if there are 429, slow down
			//   - if there are 200, reduce sleep time

			defer resp.Body.Close()
			io.Copy(ioutil.Discard, resp.Body)
			took := time.Now().Sub(start)
			if resp.StatusCode == http.StatusTooManyRequests {
				fmt.Println("#"+index+"(FAILED)", resp.Status, took)
				index += "(R)"
				time.Sleep(time.Minute)
			} else {
				fmt.Println("#"+index, resp.Status, took)
				break
			}
		}
	}
}
func main() {
	cl, err := maxcapacity.New("api.example.com", 443, 8)
	if err != nil {
		panic(err)
	}
	// TODO:
	// - use a rate limiter for each IP
	// - one client per IP?
	// Use the pool with a function,
	// set 10 to the capacity of goroutine pool and 1 second for expired duration.
	var wg sync.WaitGroup
	pool, _ := ants.NewTimingPoolWithFunc(80, 180, func(i interface{}) {
		defer wg.Done()
		myFunc(cl, i)
	})
	defer pool.Release()
	///
	howMany := 10000
	for i := 0; i < howMany; i++ {
		wg.Add(1)
		index := strconv.Itoa(i)

		if cl.IsWaiting() {
			for {
				if cl.IsWaiting() {
					time.Sleep(time.Second)
				} else {
					break
				}
			}
		}
		pool.Invoke(index)
		///
	}

	fmt.Println("waiting for completion")
	wg.Wait()
	took := time.Now().Sub(start)
	fmt.Println(
		"completed in", took,
		(float64(howMany) / (took.Seconds())), "/s",
	)

	time.Sleep(time.Hour)
}

func example() {

	cl, err := maxcapacity.New("api.example.com", 443, 8)
	if err != nil {
		panic(err)
	}
	resp, err := cl.GetWithRetry("https://api.example.com")
	if err != nil {
		panic(err)
	}
	if resp == nil {
		panic("resp is nil")
	}
	spew.Dump(resp.Status)
}
