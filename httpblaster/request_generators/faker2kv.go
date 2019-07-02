package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"runtime"
	"sync"
)

var faker = data_generator.Faker{}


type Faker2kv struct {
	workload config.Workload
	RequestCommon
}

func (self *Faker2kv) UseCommon(c RequestCommon) {

}

func (self *Faker2kv) generate_request(ch_records chan []string, ch_req chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int,wl config.Workload) {
	defer wg.Done()
	for i:=0;i<wl.Count ; i++ {

		var contentType string = "text/html"
		faker.GenerateRandomData()
		json_payload := faker.ConvertToIgzEmdItemJson()
		req := AcquireRequest()
		self.PrepareRequest(contentType, self.workload.Header, "PUT",
		self.base_uri, json_payload, host, req.Request)
		ch_req <- req
	}
}

func (self *Faker2kv) generate(ch_req chan *Request, payload string, host string,wl config.Workload) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg , c ,wl)
	}

	close(ch_records)
	wg.Wait()
}

func (self *Faker2kv) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {

	self.workload = wl

	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host,wl)

	return ch_req
}


