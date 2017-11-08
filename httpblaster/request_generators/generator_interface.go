package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
)

type Generator interface {
	UseCommon(c RequestCommon)
	GenerateRequests(global config.Global, workload config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request
}
