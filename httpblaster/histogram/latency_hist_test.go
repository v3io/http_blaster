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
