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
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"sync"
	"time"
)

type executor_result struct {
	Total    uint64
	Duration time.Duration
	Min      time.Duration
	Max      time.Duration
	Avg      time.Duration
	Iops     uint64
	Latency  map[int]int64
	Statuses map[int]int
	Errors   map[string]int
}

type executor struct {
	connections int32
	Workload    workload
	host        string
	port        string
	results     executor_result
	workers     []*worker
	Start_time  time.Time
}

func (self *executor) run(wg *sync.WaitGroup) error {
	defer wg.Done()
	self.Start_time = time.Now()
	workers_wg := sync.WaitGroup{}

	var req_per_worker uint64 = ^uint64(0)
	if self.Workload.Count > 0 {
		req_per_worker = self.Workload.Count / uint64(self.Workload.Workers)
	}
	if req_per_worker == 0 {
		self.Workload.Workers = 1
		req_per_worker = self.Workload.Count
	}
	workers_wg.Add(self.Workload.Workers)
	for i := 0; i < self.Workload.Workers; i++ {
		var url string = " "
		if config.Global.TSLMode {
			url = fmt.Sprintf("https://%s/%s/%s", self.host, self.Workload.Bucket, self.Workload.File_path)
		} else {
			url = fmt.Sprintf("http://%s/%s/%s", self.host, self.Workload.Bucket, self.Workload.File_path)
		}

		l := worker_load{req_count: req_per_worker, duration: self.Workload.Duration,
			port: config.Global.Port}
		var payload []byte
		var ferr error
		if self.Workload.Payload != "" {
			payload, ferr = ioutil.ReadFile(self.Workload.Payload)
			if ferr != nil {
				log.Fatal(ferr)
			}
		} else {
			if self.Workload.Type == PUT || self.Workload.Type == POST {
				payload = bytes.NewBuffer(dataBfr).Bytes()

			}
		}
		var contentType string = "text/html"
		l.Prepare_request(contentType, self.Workload.Header, string(self.Workload.Type),
			url, string(payload))
		server := fmt.Sprintf("%s:%s", config.Global.Server, config.Global.Port)
		w := NewWorker(server, config.Global.TSLMode)
		self.workers = append(self.workers, w)
		go w.run_worker(&l, &workers_wg)
	}
	workers_wg.Wait()
	self.results.Duration = time.Now().Sub(self.Start_time)
	self.results.Min = time.Duration(time.Second * 10)
	self.results.Max = 0
	self.results.Avg = 0
	self.results.Total = 0
	self.results.Iops = 0

	for _, w := range self.workers {
		self.results.Total += w.results.count
		if w.results.min < self.results.Min {
			self.results.Min = w.results.min
		}
		if w.results.max > self.results.Max {
			self.results.Max = w.results.max
		}
	}
	for _, w := range self.workers {
		self.results.Avg +=
			time.Duration(float64(w.results.count) / float64(self.results.Total) * float64(w.results.avg))
		for k, v := range w.results.codes {
			self.results.Statuses[k] += v
		}

	}
	seconds := uint64(self.results.Duration.Seconds())
	if seconds == 0 {
		seconds = 1
	}
	self.results.Iops = self.results.Total / seconds

	log.Println("Ending ", self.Workload.Name)

	return nil
}

func (self *executor) Start(wg *sync.WaitGroup) error {
	self.results.Statuses = make(map[int]int)
	log.Println("at executor start ", self.Workload)
	go func() {
		self.run(wg)
	}()
	return nil
}

func (self *executor) Stop() error {
	return errors.New("Not Implimented!!!")
}

func (self *executor) Report() (executor_result, error) {
	log.Println("report for wl ", self.Workload.Id, ":")
	log.Println("Total Requests ", self.results.Total)
	log.Println("Min: ", self.results.Min)
	log.Println("Max: ", self.results.Max)
	log.Println("Avg: ", self.results.Avg)
	log.Println("Statuses: ")
	for k, v := range self.results.Statuses {
		log.Println(fmt.Sprintf("%d - %d", k, v))
	}

	log.Println("iops: ", self.results.Iops)
	for err_code, err_count := range self.results.Statuses {
		if max_errors, ok := config.Global.StatusCodesDist[strconv.Itoa(err_code)]; ok {
			if self.results.Total > 0 && err_count > 0 {
				err_percent := (float64(err_count) * float64(100)) / float64(self.results.Total)
				log.Printf("errors type %d occured %f%% during the test \"%s\"",
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
	return self.results, nil
}
