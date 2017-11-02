package tui

import (
	"github.com/sasile/gohistogram"
	"time"
)

type LatencyCollector struct {
	WeighHist *gohistogram.NumericHistogram
	ch_values chan time.Duration
}

func (self *LatencyCollector)New(n int, alpha float64)  chan time.Duration{
	self.WeighHist = gohistogram.NewHistogram(400)
	self.ch_values = make(chan time.Duration, 400000)
	go func() {
		for v := range self.ch_values{
			self.WeighHist.Add( v.Seconds()*1000 )
		}
	}()
	return self.ch_values
}

func (self *LatencyCollector)Add(v time.Duration) {
	self.ch_values <- v
}

func (self *LatencyCollector)Get()([]string, []int)  {
	return self.WeighHist.BarArray()

}