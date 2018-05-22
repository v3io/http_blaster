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
