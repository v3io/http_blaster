package request_generators

import (
	"bufio"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type Json2KV struct {
	workload config.Workload
	base_uri string
	RequestCommon
}

func (self *Json2KV) UseCommon(c RequestCommon) {

}

func (self *Json2KV) generate_request(ch_records chan []byte, ch_req chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	parser := igz_data.EmdSchemaParser{}
	var contentType string = "text/html"
	e := parser.LoadSchema(self.workload.Schema, self.workload.KeyFields, self.workload.KeyFormat)
	if e != nil {
		panic(e)
	}
	for r := range ch_records {
		json_payload, err := parser.EmdFromJsonRecord(r)
		if err != nil {
			panic(err)
		}
		req := AcquireRequest()
		self.PrepareRequest(contentType, self.workload.Header, string(self.workload.Type),
			self.base_uri, json_payload, host, req.Request)
		ch_req <- req
	}
}

func (self *Json2KV) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []byte = make(chan []byte)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}
	ch_files := self.FilesScan(self.workload.Payload)

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var i int = 0
			for {
				line, _, err := reader.ReadLine()
				if err == nil {
					ch_records <- line
					i++
				} else if err == io.EOF {
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
	wg.Wait()
}

func (self *Json2KV) GenerateRequests(wl config.Workload, tls_mode bool, host string) chan *Request {
	self.workload = wl
	//panic(fmt.Sprintf("workload key [%s] workload key sep [%s]", wl.KeyFormat, string(wl.KeyFormatSep.Rune)))
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	if tls_mode {
		self.base_uri = fmt.Sprintf("https://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	} else {
		self.base_uri = fmt.Sprintf("http://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	}

	ch_req := make(chan *Request, 1000)


	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
