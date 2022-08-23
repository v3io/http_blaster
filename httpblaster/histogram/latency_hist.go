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
package histogram

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"sort"
	"sync"
	"time"
)

type LatencyHist struct {
	ch_values chan time.Duration
	hist      map[int64]int
	count     int64
	size      int64
	wg        sync.WaitGroup
}

func (self *LatencyHist) Add(v time.Duration) {
	self.ch_values <- v
	self.size++
}

func (self *LatencyHist) Close() {
	close(self.ch_values)
}

func (self *LatencyHist) place(v int64) {
	self.hist[v/100]++
}

func (self *LatencyHist) New() chan time.Duration {
	log.Debugln("new latency hist")
	self.hist = make(map[int64]int)
	self.wg.Add(1)

	self.ch_values = make(chan time.Duration, 10000)

	go func() {
		defer self.wg.Done()
		for v := range self.ch_values {
			self.count++
			self.place(v.Nanoseconds() / 1000)
		}
	}()
	return self.ch_values
}

func (self *LatencyHist) GetResults() ([]string, []float64) {
	log.Debugln("get latency hist")
	self.wg.Wait()
	var keys []int
	for k := range self.hist {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	res_strings := []string{}
	res_values := []float64{}
	for _, k := range keys {
		v := self.hist[int64(k)]
		res_strings = append(res_strings, fmt.Sprintf("%5d - %5d",
			k*100, (k+1)*100))
		value := float64(v*100) / float64(self.count)
		res_values = append(res_values, value)
	}
	return res_strings, res_values
}

func (self *LatencyHist) GetHistMap() map[int64]int {
	self.wg.Wait()
	return self.hist
}
