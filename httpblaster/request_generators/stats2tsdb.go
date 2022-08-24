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
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"runtime"
	"strconv"
	"sync"
)

var gen = data_generator.MemoryGenerator{}


type Stats2TSDB struct {
	workload config.Workload
	RequestCommon
}

func (self *Stats2TSDB) UseCommon(c RequestCommon) {

}

func (self *Stats2TSDB) generate_request(ch_records chan []string, ch_req chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int,wl config.Workload) {
		defer wg.Done()
		for i:=0;i<wl.Count ; i++ {

			var contentType string = "text/html"
			json_payload := gen.GenerateRandomData(strconv.FormatInt(int64(i), 10))
			for _, payload := range json_payload {
				req := AcquireRequest()
				self.PrepareRequest(contentType, self.workload.Header, "PUT",
					self.base_uri, payload, host, req.Request)
				ch_req <- req
			}
		}
}

func (self *Stats2TSDB) generate(ch_req chan *Request, payload string, host string,wl config.Workload) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg , c ,wl)
	}

	close(ch_records)
	wg.Wait()
}

func (self *Stats2TSDB) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {

	self.workload = wl

	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host,wl)

	return ch_req
}


