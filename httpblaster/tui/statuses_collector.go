package tui

import (
	"github.com/sasile/gohistogram"
)

type StatusesCollector struct {
	WeighHist *gohistogram.NumericHistogram
	ch_values chan int
}

func (self *StatusesCollector) New(n int, alpha float64) chan int {
	self.WeighHist = gohistogram.NewHistogram(10)
	self.ch_values = make(chan int, 400000)
	go func() {
		for v := range self.ch_values {
			self.WeighHist.Add(float64(v))
		}
	}()
	return self.ch_values
}

func (self *StatusesCollector) Get() ([]string, []int) {
	return self.WeighHist.BarArray()

}
