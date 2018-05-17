package histogram

import (
	"time"
	"strconv"
	"sync"
	log "github.com/sirupsen/logrus"
)

type LatencyHist struct {
	ch_values chan time.Duration
	hist map[int]int
	count int64
	wg sync.WaitGroup
}


func (self *LatencyHist) Add(v time.Duration) {
	log.Debugln("values added")
	self.ch_values <- v
}

func (self *LatencyHist) place(v float64)  {
	self.hist[int(v/100)]++
}

func (self *LatencyHist)New()chan time.Duration {
	log.Debugln("new latency hist")
	self.hist = make(map[int]int)
	self.wg.Add(1)

	self.ch_values = make(chan time.Duration, 10000)
	go func() {
		defer self.wg.Done()
		for v := range self.ch_values {
			self.count++
			self.place(float64(v.Nanoseconds() / 1000))
		}
	}()
	return self.ch_values
}

func (self *LatencyHist) GetResults() ([]string, []float64) {
	log.Debugln("get latency hist")
	self.wg.Wait()
	log.Debugln("latency hist wait released")
	res_strings := [] string{}
	res_values := []float64{}
	for k,v := range self.hist{
		res_strings = append(res_strings, strconv.Itoa(k*100) )
		res_values = append(res_values,float64(v * 100) / float64(self.count))
	}
	return res_strings, res_values
}
