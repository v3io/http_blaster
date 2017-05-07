package request_generators

import (
	"encoding/csv"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/schema_parser"
	"github.com/valyala/fasthttp"
	"io"
	"os"
	"runtime"
)

type Csv2KV struct {
	workload config.Workload
	base_uri string
	RequestCommon
}

func (self *Csv2KV) UseCommon(c RequestCommon) {

}

func (self *Csv2KV) generate_request(ch_records chan []string, ch_req chan *fasthttp.Request, host string) {
	parser := schema_parser.SchemaParser{}
	var contentType string = "text/html"
	e := parser.LoadSchema(self.workload.Schema)
	if e != nil {
		panic(e)
	}
	for r := range ch_records {
		json_payload := parser.JsonFromCSVRecord(r)
		req := self.PrepareRequest(contentType, self.workload.Header, string(self.workload.Type),
			self.base_uri, json_payload, host)
		ch_req <- req
	}
}

func (self *Csv2KV) generate(ch_req chan *fasthttp.Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string)
	defer close(ch_records)

	f, err := os.Open(self.workload.Payload)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = self.workload.Separator.Rune

	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host)
	}

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		ch_records <- record
	}
}

func (self *Csv2KV) GenerateRequests(wl config.Workload, tls_mode bool, host string) chan *fasthttp.Request {
	self.workload = wl
	self.workload.Header["X-v3io-function"] = "PutItem"

	if tls_mode {
		self.base_uri = fmt.Sprintf("https://%s/%s/%s", host, self.workload.Bucket, self.workload.File_path)
	} else {
		self.base_uri = fmt.Sprintf("http://%s/%s/%s", host, self.workload.Bucket, self.workload.File_path)
	}
	ch_req := make(chan *fasthttp.Request, 1000)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
