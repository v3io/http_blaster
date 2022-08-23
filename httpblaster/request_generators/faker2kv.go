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
	"github.com/v3io/http_blaster/httpblaster/utils"
	"runtime"
	"sync"
	"time"
)

var faker = data_generator.Fake{}

type Faker2KV struct {
	workload config.Workload
	RequestCommon
}

func (receiver *Faker2KV) UseCommon(c RequestCommon) {

}

func (receiver *Faker2KV) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retCh chan *Response, workerQd int) chan *Request {
	// generating partition
	t := time.Now().UTC().AddDate(0, 0, 0)
	part := ""
	if wl.Partition != "" {
		part = receiver.GenerateCurrentPartition(wl.Partition, t)
	}

	receiver.workload = wl

	if receiver.workload.Header == nil {
		receiver.workload.Header = make(map[string]string)
	}
	receiver.workload.Header["X-v3io-function"] = "PutItem"

	receiver.SetBaseUri(tlsMode, host, receiver.workload.Container, receiver.workload.Target+part)

	ch_req := make(chan *Request, workerQd)

	go receiver.generate(ch_req, receiver.workload.Payload, host, wl, tlsMode)

	return ch_req
}

func (receiver *Faker2KV) generate(ch_req chan *Request, payload string, host string, wl config.Workload, tlsMode bool) {
	defer close(ch_req)
	var chRecords = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go receiver.generateRequest(chRecords, ch_req, host, &wg, c, wl, tlsMode)
	}

	close(chRecords)
	wg.Wait()
}

func (receiver *Faker2KV) generateRequest(chRecords chan []string, chReq chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int, wl config.Workload, tlsMode bool) {
	defer wg.Done()
	faker.Init()
	for i := 0; i < wl.Count; i++ {

		//receiver.SetBaseUri(tlsMode, host, receiver.workload.Container, receiver.workload.Target+part)
		var contentType = "text/html"
		current_time := time.Now().UTC()
		faker.GenerateRandomData(current_time)
		jsonPayload := faker.ConvertToIgzEmdItemJson()
		part := receiver.GenerateCurrentPartition(receiver.workload.Partition, current_time)
		receiver.SetBaseUri(tlsMode, host, receiver.workload.Container, receiver.workload.Target+part)
		req := AcquireRequest()
		receiver.PrepareRequest(contentType, receiver.workload.Header, "PUT",
			receiver.base_uri, jsonPayload, host, req.Request)
		chReq <- req
	}
}

func (receiver *Faker2KV) GenerateCurrentPartition(partitionBy string, t time.Time) string {
	return utils.GeneratePartitionedRequest(partitionBy, t)
}
