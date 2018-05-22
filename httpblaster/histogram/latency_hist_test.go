package histogram

import (
	"math/rand"
	"testing"
	"time"
)

func TestLatencyHist_Get(t *testing.T) {
	l := LatencyHist{}
	c := l.New()
	req := 1000000

	go func() {
		for i := 0; i < req; i++ {
			l.Add(time.Microsecond * time.Duration(rand.Intn(2000)))

		}
		close(c)
	}()

	s, v := l.GetResults()
	total := float64(0)
	for i, _ := range s {
		total += v[i]
		t.Logf("%6v(us)\t\t%3.2f%%", s[i], v[i])
	}
	t.Logf("Total: %3.3f", total)
}
