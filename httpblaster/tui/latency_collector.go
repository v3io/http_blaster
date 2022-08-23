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
package tui

import (
	"github.com/sasile/gohistogram"
	"time"
)

type LatencyCollector struct {
	WeighHist *gohistogram.NumericHistogram
	ch_values chan time.Duration
}

func (self *LatencyCollector) New(n int, alpha float64) chan time.Duration {
	self.WeighHist = gohistogram.NewHistogram(50)
	self.ch_values = make(chan time.Duration, 400000)
	go func() {
		for v := range self.ch_values {
			self.WeighHist.Add(float64(v.Nanoseconds() / 1000))
		}
	}()
	return self.ch_values
}

func (self *LatencyCollector) Add(v time.Duration) {
	self.ch_values <- v
}

func (self *LatencyCollector) Get() ([]string, []int) {
	return self.WeighHist.BarArray()
}

func (self *LatencyCollector) GetResults() ([]string, []float64) {
	return self.WeighHist.GetHistAsArray()

}

func (self *LatencyCollector) GetQuantile(q float64) float64 {
	return self.WeighHist.CDF(q)

}

func (self *LatencyCollector) GetCount() float64 {
	return self.WeighHist.Count()

}

func (self *LatencyCollector) String() string {
	return self.WeighHist.String()

}
