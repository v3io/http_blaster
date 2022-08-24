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
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
)

type Line2HttpGenerator struct {
	RequestCommon
	workload config.Workload
}

func (self *Line2HttpGenerator) UseCommon(c RequestCommon) {

}

func (self *Line2HttpGenerator) generate_request(ch_lines chan string,
	ch_req chan *Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/text"
	for r := range ch_lines {
		req := AcquireRequest()
		self.PrepareRequest(contentType, self.workload.Header, "PUT",
			self.base_uri, r, host, req.Request)
		ch_req <- req
	}
	log.Println("generate_request Done")
}

func (self *Line2HttpGenerator) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	ch_lines := make(chan string, 10000)
	wg := sync.WaitGroup{}
	ch_files := self.FilesScan(self.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_lines, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var line_count int = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_lines <- strings.TrimSpace(line)
					line_count++
					if line_count%1024 == 0 {
						log.Printf("line: %d from file %s was submitted", line_count, f)
					}
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", line_count))
		} else {
			panic(err)
		}
	}
	close(ch_lines)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *Line2HttpGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	//self.workload.Header["X-v3io-function"] = "PutRecords"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
