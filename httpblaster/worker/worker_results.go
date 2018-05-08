package worker

import "time"

type worker_results struct {
	Count              uint64
	Min                time.Duration
	Max                time.Duration
	Avg                time.Duration
	Read               uint64
	Write              uint64
	Codes              map[int]uint64
	Method             string
	ConnectionRestarts uint32
	ErrorCount         uint32
}
