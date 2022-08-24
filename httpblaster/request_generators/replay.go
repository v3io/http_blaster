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
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
)

type Replay struct {
	workload config.Workload
	RequestCommon
}

func (self *Replay) UseCommon(c RequestCommon) {

}

func (self *Replay) generate_request(ch_records chan []byte, ch_req chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range ch_records {
		req_dump := &RequestDump{}
		json.Unmarshal(r, req_dump)

		req := AcquireRequest()
		self.PrepareRequest(contentType, req_dump.Headers,
			req_dump.Method,
			req_dump.URI,
			req_dump.Body,
			req_dump.Host,
			req.Request)
		ch_req <- req
	}
}

func (self *Replay) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []byte = make(chan []byte, 10000)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}
	r_count := 0
	ch_files := self.FilesScan(self.workload.Payload)

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			r_count++
			reader := bufio.NewReader(file)
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				log.Errorf("Fail to read file %v:%v", f, err.Error())
			} else {
				ch_records <- data
			}
			log.Println(fmt.Sprintf("Finish file scaning, generated %d requests ", r_count))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	wg.Wait()
}

func (self *Replay) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl

	ch_req := make(chan *Request, worker_qd)

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
