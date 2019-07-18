package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"github.com/v3io/http_blaster/httpblaster/utils"
	"runtime"
	"sync"
)

var faker = data_generator.Fake{}

type Faker2KV struct {
	workload config.Workload
	RequestCommon
}

func (receiver *Faker2KV) UseCommon(c RequestCommon) {

}

func (receiver *Faker2KV) generateRequest(chRecords chan []string, chReq chan *Request, host string,
	wg *sync.WaitGroup, cpuNumber int, wl config.Workload) {
	defer wg.Done()
	faker.Init()
	for i := 0; i < wl.Count; i++ {

		var contentType = "text/html"
		faker.GenerateRandomData()
		jsonPayload := faker.ConvertToIgzEmdItemJson()
		req := AcquireRequest()
		receiver.PrepareRequest(contentType, receiver.workload.Header, "PUT",
			receiver.base_uri, jsonPayload, host, req.Request)
		chReq <- req
	}
}

func (receiver *Faker2KV) generate(ch_req chan *Request, payload string, host string, wl config.Workload) {
	defer close(ch_req)
	var chRecords = make(chan []string, 1000)
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go receiver.generateRequest(chRecords, ch_req, host, &wg, c, wl)
	}

	close(chRecords)
	wg.Wait()
}

func (receiver *Faker2KV) GenerateRequests(global config.Global, wl config.Workload, tlsMode bool, host string, retCh chan *Response, workerQd int) chan *Request {
	// generating partition
	part := ""
	if wl.Partition != "" {
		part = receiver.GenerateCurrentPartition(wl.Partition)
	}

	receiver.workload = wl

	if receiver.workload.Header == nil {
		receiver.workload.Header = make(map[string]string)
	}
	receiver.workload.Header["X-v3io-function"] = "PutItem"

	receiver.SetBaseUri(tlsMode, host, receiver.workload.Container, receiver.workload.Target+part)

	ch_req := make(chan *Request, workerQd)

	go receiver.generate(ch_req, receiver.workload.Payload, host, wl)

	return ch_req
}

func (receiver *Faker2KV) GenerateCurrentPartition(partitionBy string) string {
	return utils.GeneratePartitionedRequest(partitionBy)
}
