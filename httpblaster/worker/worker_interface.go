package worker

import (
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"sync"
	"time"
)

type Worker interface {
	UseBase(c WorkerBase)
	Init(lazy int)
	GetResults() worker_results
	RunWorker(ch_resp chan *request_generators.Response,
		ch_req chan *request_generators.Request,
		wg *sync.WaitGroup,
		release_req bool,
		ch_latency chan time.Duration,
		ch_statuses chan int,
		dump_requests bool,
		dump_location string)
}
