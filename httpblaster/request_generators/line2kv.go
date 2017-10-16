package request_generators

import (
	"bufio"
	"fmt"
	//"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	//"github.com/v3io/http_blaster/httpblaster/igz_data"
	"github.com/valyala/fasthttp"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

type Line2KvGenerator struct {
	RequestCommon
	workload config.Workload
}

func (self *Line2KvGenerator) UseCommon(c RequestCommon) {

}

func (self *Line2KvGenerator) generate_request(ch_records chan []string,
	ch_req chan *fasthttp.Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range ch_records {
		req := self.PrepareRequest(contentType, self.workload.Header, "PUT",
			r[0], r[1], host)
		//panic(fmt.Sprintf("%+v",r))
		ch_req <- req
	}
	log.Println("generate_request Done")
}

func (self *Line2KvGenerator) generate(ch_req chan *fasthttp.Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string)
	wg := sync.WaitGroup{}
	ch_files := self.FilesScan(self.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int = 0
			for {
				address, addr_err := reader.ReadString('\n')
				payload, payload_err := reader.ReadString('\n')

				if addr_err == nil && payload_err == nil {
					ch_records <- []string{strings.TrimSpace(address), string(payload)}
					i++
				} else if addr_err == io.EOF || payload_err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", i))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *Line2KvGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, worker_qd int) chan *fasthttp.Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *fasthttp.Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
