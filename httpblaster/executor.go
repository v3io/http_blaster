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
package httpblaster

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"github.com/v3io/http_blaster/httpblaster/tui"
	"github.com/v3io/http_blaster/httpblaster/worker"
	"strconv"
	"strings"
	"sync"
	"time"
)

type executor_result struct {
	Total       uint64
	Duration    time.Duration
	Min         time.Duration
	Max         time.Duration
	Avg         time.Duration
	Iops        uint64
	Latency     map[int]int64
	Statuses    map[int]uint64
	Errors      map[string]int
	ErrorsCount uint32
	ConRestarts uint32
}

type Executor struct {
	connections int32
	Workload    config.Workload
	Globals     config.Global
	//host                  string
	//port                  string
	//tls_mode              bool
	Host  string
	Hosts []string
	//Port                  string
	TLS_mode       bool
	results        executor_result
	workers        []worker.Worker
	Start_time     time.Time
	Data_bfr       []byte
	WorkerQd       int
	TermUi         *tui.Term_ui
	Ch_get_latency chan time.Duration
	Ch_put_latency chan time.Duration
	//Ch_statuses    chan int
	DumpFailures bool
	DumpLocation string
}

func (self *Executor) load_request_generator() (chan *request_generators.Request,
	bool, chan *request_generators.Response) {
	var req_gen request_generators.Generator
	var release_req bool = true
	var ch_response chan *request_generators.Response = nil

	gen_type := strings.ToLower(self.Workload.Generator)
	switch gen_type {
	case request_generators.PERFORMANCE:
		req_gen = &request_generators.PerformanceGenerator{}
		if self.Workload.FilesCount == 0 {
			release_req = false
		}
		break
	case request_generators.LINE2STREAM:
		req_gen = &request_generators.Line2StreamGenerator{}
		break
	case request_generators.CSV2KV:
		req_gen = &request_generators.Csv2KV{}
		break
	case request_generators.CSVUPDATEKV:
		req_gen = &request_generators.CsvUpdateKV{}
		break
	case request_generators.JSON2KV:
		req_gen = &request_generators.Json2KV{}
		break
	case request_generators.LINE2KV:
		req_gen = &request_generators.Line2KvGenerator{}
		break
	case request_generators.RESTORE:
		req_gen = &request_generators.RestoreGenerator{}
		break
	case request_generators.CSV2STREAM:
		req_gen = &request_generators.CSV2StreamGenerator{}
		break
	case request_generators.LINE2HTTP:
		req_gen = &request_generators.Line2HttpGenerator{}
		break
	case request_generators.REPLAY:
		req_gen = &request_generators.Replay{}
		break
	case request_generators.STREAM_GET:
		req_gen = &request_generators.StreamGetGenerator{}
		ch_response = make(chan *request_generators.Response)
	default:
		panic(fmt.Sprintf("unknown request generator %s", self.Workload.Generator))
	}
	var host string
	if len(self.Hosts) > 0 {
		host = self.Hosts[0]
	} else {
		host = self.Host
	}

	ch_req := req_gen.GenerateRequests(self.Globals, self.Workload, self.TLS_mode, host, nil, self.WorkerQd)
	return ch_req, release_req, ch_response
}

func (self *Executor) GetWorkerType() worker.WorkerType {
	gen_type := strings.ToLower(self.Workload.Generator)
	if gen_type == request_generators.PERFORMANCE {
		return worker.PERFORMANCE_WORKER
	}
	return worker.INGESTION_WORKER
}

func (self *Executor) GetType() string {
	return self.Workload.Type
}

func (self *Executor) run(wg *sync.WaitGroup) error {
	defer wg.Done()
	self.Start_time = time.Now()
	workers_wg := sync.WaitGroup{}
	workers_wg.Add(self.Workload.Workers)

	ch_req, release_req_flag, ch_response := self.load_request_generator()

	for i := 0; i < self.Workload.Workers; i++ {
		var host_address string
		if len(self.Hosts) > 0 {
			server_id := (i) % len(self.Hosts)
			host_address = self.Hosts[server_id]
		} else {
			host_address = self.Host
		}

		server := fmt.Sprintf("%s:%s", host_address, self.Globals.Port)
		w := worker.NewWorker(self.GetWorkerType(),
			server, self.Globals.TLSMode, self.Workload.Lazy,
			self.Globals.RetryOnStatusCodes,
			self.Globals.RetryCount, self.Globals.PemFile, i, self.Workload.Name)
		self.workers = append(self.workers, w)
		//var ch_latency chan time.Duration
		//if self.Workload.Type == "GET" {
		//	ch_latency = self.Ch_get_latency
		//} else {
		//	ch_latency = self.Ch_put_latency
		//}

		go w.RunWorker(ch_response, ch_req,
			&workers_wg, release_req_flag, // ch_latency,
			//self.Ch_statuses,
			self.DumpFailures,
			self.DumpLocation)
	}
	ended := make(chan bool)
	go func() {
		workers_wg.Wait()
		close(ended)
	}()
	tick := time.Tick(time.Millisecond * 500)
LOOP:
	for {
		select {
		case <-ended:
			break LOOP
		case <-tick:
			if self.TermUi != nil {
				var put_req_count uint64 = 0
				var get_req_count uint64 = 0
				for _, w := range self.workers {
					wresults := w.GetResults()
					if w.GetResults().Method == `PUT` {
						put_req_count += wresults.Count
					} else {
						get_req_count += wresults.Count
					}
				}
				self.TermUi.Update_requests(time.Now().Sub(self.Start_time), put_req_count, get_req_count)
			}
		}
	}

	self.results.Duration = time.Now().Sub(self.Start_time)
	self.results.Min = time.Duration(time.Second * 10)
	self.results.Max = 0
	self.results.Avg = 0
	self.results.Total = 0
	self.results.Iops = 0

	for _, w := range self.workers {
		wresults := w.GetResults()
		self.results.ConRestarts += wresults.ConnectionRestarts
		self.results.ErrorsCount += wresults.ErrorCount

		self.results.Total += wresults.Count
		if w.GetResults().Min < self.results.Min {
			self.results.Min = wresults.Min
		}
		if w.GetResults().Max > self.results.Max {
			self.results.Max = wresults.Max
		}

		self.results.Avg +=
			time.Duration(float64(wresults.Count) / float64(self.results.Total) * float64(wresults.Avg))
		for k, v := range wresults.Codes {
			self.results.Statuses[k] += v
		}
	}

	seconds := uint64(self.results.Duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	self.results.Iops = self.results.Total / seconds

	log.Info("Ending ", self.Workload.Name)

	return nil
}

func (self *Executor) Start(wg *sync.WaitGroup) error {
	self.results.Statuses = make(map[int]uint64)
	log.Info("at executor start ", self.Workload)
	go func() {
		self.run(wg)
	}()
	return nil
}

func (self *Executor) Stop() error {
	return errors.New("Not Implimented!!!")
}

func (self *Executor) Report() (executor_result, error) {
	log.Info("report for wl ", self.Workload.Id, ":")
	log.Info("Total Requests ", self.results.Total)
	log.Info("Min: ", self.results.Min)
	log.Info("Max: ", self.results.Max)
	log.Info("Avg: ", self.results.Avg)
	log.Info("Connection Restarts: ", self.results.ConRestarts)
	log.Info("Error Count: ", self.results.ErrorsCount)
	log.Info("Statuses: ")
	for k, v := range self.results.Statuses {
		log.Println(fmt.Sprintf("%d - %d", k, v))
	}

	log.Info("iops: ", self.results.Iops)
	for err_code, err_count := range self.results.Statuses {
		if max_errors, ok := self.Globals.StatusCodesAcceptance[strconv.Itoa(err_code)]; ok {
			if self.results.Total > 0 && err_count > 0 {
				err_percent := (float64(err_count) * float64(100)) / float64(self.results.Total)
				log.Infof("status code %d occured %f%% during the test \"%s\"",
					err_code, err_percent, self.Workload.Name)
				if float64(err_percent) > float64(max_errors) {
					return self.results,
						errors.New(fmt.Sprintf("Executor %s completed with errors: %+v",
							self.Workload.Name, self.results.Statuses))
				}
			}
		} else {
			return self.results, errors.New(fmt.Sprintf("Executor %s completed with errors: %+v",
				self.Workload.Name, self.results.Statuses))
		}
	}
	if self.results.ErrorsCount > 0 {
		return self.results, errors.New("executor completed with errors")
	}
	return self.results, nil
}

func (self *Executor) LatencyHist() map[int64]int {
	res := make(map[int64]int)
	for _, w := range self.workers {
		hist := w.GetHist()
		for k, v := range hist {
			res[k] += v
		}
	}
	return res
}
