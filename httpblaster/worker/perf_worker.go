package worker

/*
Copyright 2016 Iguazio.io Systems Ltd.

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

import (
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"sync"
	//"time"
)

type PerfWorker struct {
	WorkerBase
}

func (w *PerfWorker) UseBase(c WorkerBase) {

}

func (w *PerfWorker) RunWorker(ch_resp chan *request_generators.Response, ch_req chan *request_generators.Request,
	wg *sync.WaitGroup, release_req bool,
	//ch_latency chan time.Duration,
	//ch_statuses chan int,
	dump_requests bool,
	dump_location string) {
	defer wg.Done()
	var req_type sync.Once

	do_once.Do(func() {
		log.Info("Running Performance workers")
	})

	response := request_generators.AcquireResponse()
	defer request_generators.ReleaseResponse(response)
	for req := range ch_req {
		req_type.Do(func() {
			w.Results.Method = string(req.Request.Header.Method())
		})
		err, _ := w.send_request(req, response)

		if err != nil {
			log.Errorf("send request failed %s", err.Error())
		}

		//ch_statuses <- response.Response.StatusCode()
		//ch_latency <- d
		request_generators.ReleaseRequest(req)
		response.Response.Reset()
	}
	log.Debugln("closing hist")
	w.hist.Close()
	w.close_connection()
}
