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
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"github.com/valyala/fasthttp"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

var once sync.Once
var dump_dir string

type IngestWorker struct {
	WorkerBase
}

func (w *IngestWorker) UseBase(c WorkerBase) {

}
func (w *IngestWorker) dump_requests(ch_dump chan *fasthttp.Request, dump_location string,
	sync_dump *sync.WaitGroup) {

	once.Do(func() {
		t := time.Now()
		dump_dir = path.Join(dump_location, fmt.Sprintf("BlasterDump-%v", t.Format("2006-01-02-150405")))
		err := os.Mkdir(dump_dir, 0777)
		if err != nil {
			log.Errorf("Fail to create dump dir %v:%v", dump_dir, err.Error())
		}
	})
	defer sync_dump.Done()

	i := 0
	for r := range ch_dump {
		file_name := fmt.Sprintf("w%v_request_%v", w.id, i)
		file_path := filepath.Join(dump_dir, file_name)
		log.Info("generating dump file ", file_path)
		i++
		file, err := os.Create(file_path)
		if err != nil {
			log.Errorf("Fail to open file %v for request dump: %v", file_path, err.Error())
		} else {
			rdump := &request_generators.RequestDump{}
			rdump.Host = string(r.Host())
			rdump.Method = string(r.Header.Method())
			rdump.Body = string(r.Body())
			rdump.URI = r.URI().String()
			rdump.Headers = make(map[string]string)
			r.Header.VisitAll(func(key, value []byte) {
				rdump.Headers[string(key)] = string(value)
			})
			jsonStr, err := json.Marshal(rdump)
			if err != nil {
				log.Errorf("Fail to dump request %v", err.Error())
			}
			log.Debug("Write dump request")
			file.Write(jsonStr)
			file.Close()
		}
	}
}

func (w *IngestWorker) RunWorker(ch_resp chan *request_generators.Response, ch_req chan *request_generators.Request,
	wg *sync.WaitGroup, release_req bool,
	//ch_latency chan time.Duration,
	//ch_statuses chan int,
	dump_requests bool,
	dump_location string) {
	defer wg.Done()
	var onceSetRequest sync.Once
	var oncePrepare sync.Once
	var request *request_generators.Request
	submit_request := request_generators.AcquireRequest()
	var req_type sync.Once
	var ch_dump chan *fasthttp.Request
	var sync_dump sync.WaitGroup

	do_once.Do(func() {
		log.Info("Running Ingestion workers")
	})

	if dump_requests {
		ch_dump = make(chan *fasthttp.Request, 100)
		sync_dump.Add(1)
		go w.dump_requests(ch_dump, dump_location, &sync_dump)
	}

	prepareRequest := func() {
		request.Request.Header.CopyTo(&submit_request.Request.Header)
		submit_request.Request.AppendBody(request.Request.Body())
		submit_request.Request.SetHost(w.host)
	}

	for req := range ch_req {
		req_type.Do(func() {
			w.Results.Method = string(req.Request.Header.Method())
		})

		if release_req {
			req.Request.SetHost(w.host)
			submit_request = req
		} else {
			onceSetRequest.Do(func() {
				request = req
			})
			oncePrepare.Do(prepareRequest)
		}

		var err error
		//var d time.Duration
		response := request_generators.AcquireResponse()
	LOOP:
		for i := 0; i < w.retry_count; i++ {
			err, _ = w.send_request(submit_request, response)
			if err != nil {
				//retry on error
				response.Response.Reset()
				continue
			} else {
				//ch_statuses <- response.Response.StatusCode()
				//ch_latency <- d
			}
			if response.Response.StatusCode() >= http.StatusBadRequest {
				if _, ok := w.retry_codes[response.Response.StatusCode()]; !ok {
					//not subject to retry
					break LOOP
				} else if i+1 < w.retry_count {
					//not the last loop
					response.Response.Reset()
				}
			} else {
				break LOOP
			}
		}
		//ch_statuses <- response.Response.StatusCode()
		//ch_latency <- d
		if response.Response.StatusCode() >= http.StatusBadRequest &&
			response.Response.StatusCode() < http.StatusInternalServerError &&
			dump_requests {
			//dump request
			r := fasthttp.AcquireRequest()
			r.SetBody(submit_request.Request.Body())
			submit_request.Request.CopyTo(r)
			ch_dump <- r
		}
		if ch_resp != nil {
			ch_resp <- response
		} else {
			request_generators.ReleaseResponse(response)
		}
		if release_req {
			request_generators.ReleaseRequest(req)
		}
	}
	if dump_requests {
		log.Info("wait for dump routine to end")
		close(ch_dump)
		sync_dump.Wait()
	}
	w.hist.Close()
	w.close_connection()
}
