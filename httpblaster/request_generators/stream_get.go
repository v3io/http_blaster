/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package request_generators

import (
	"bufio"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type StreamGetGenerator struct {
	RequestCommon
	workload config.Workload
}

func (self *StreamGetGenerator) UseCommon(c RequestCommon) {

}

func (self *StreamGetGenerator) generate_request(ch_records chan string,
	ch_req chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range ch_records {
		sr := igz_data.NewStreamRecord("client", r, u.String(), 0, true)
		r := igz_data.NewStreamRecords(sr)
		req := AcquireRequest()
		self.PrepareRequest(contentType, self.workload.Header, "PUT",
			self.base_uri, r.ToJsonString(), host, req.Request)
		ch_req <- req
	}
	log.Println("generate_request Done")
}

func (self *StreamGetGenerator) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan string = make(chan string)
	wg := sync.WaitGroup{}
	ch_files := self.FilesScan(self.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_records <- strings.TrimSpace(line)
					i++
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", i))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *StreamGetGenerator) NextLocationFromResponse(response *Response) interface{} {
	return 0
}

func (self *StreamGetGenerator) Consumer(return_ch chan *Response) chan interface{} {
	ch_location := make(chan interface{}, 1000)
	go func() {
		for {
			select {
			case response := <-return_ch:
				loc := self.NextLocationFromResponse(response)
				ch_location <- loc
			case <-time.After(time.Second * 30):
				log.Println("didn't get location for more then 30 seconds, exit now")
				return
			}
		}
	}()
	return ch_location
}

func (self *StreamGetGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutRecords"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
