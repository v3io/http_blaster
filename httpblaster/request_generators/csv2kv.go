package request_generators

import (
	"encoding/csv"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

type Csv2KV struct {
	workload config.Workload
	RequestCommon
}

func (self *Csv2KV) UseCommon(c RequestCommon) {

}

func (self *Csv2KV) generate_request(ch_records chan []string, ch_req chan *Request, host string,
	wg *sync.WaitGroup) {
	defer wg.Done()
	parser := igz_data.EmdSchemaParser{}
	var contentType string = "text/html"
	e := parser.LoadSchema(self.workload.Schema)
	if e != nil {
		panic(e)
	}
	for r := range ch_records {
		json_payload := parser.EmdFromCSVRecord(r)
		req := AcquireRequest()
		self.PrepareRequest(contentType, self.workload.Header, "PUT",
			self.base_uri, json_payload, host, req.Request)
		ch_req <- req
	}
}

func (self *Csv2KV) generate(ch_req chan *Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan []string = make(chan []string, 1000)
	parser := igz_data.EmdSchemaParser{}
	e := parser.LoadSchema(self.workload.Schema)
	if e != nil {
		panic(e)
	}

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}

	ch_files := self.FilesScan(self.workload.Payload)

	for f := range ch_files {
		fp, err := os.Open(f)
		if err != nil {
			panic(err)
		}

		r := csv.NewReader(fp)
		r.Comma = parser.JsonSchema.Settings.Separator.Rune
		var line_count = 0
		for {
			record, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			if strings.HasPrefix(record[0], "#") {
				log.Println("Skipping scv header ", strings.Join(record[:], ","))
			} else {
				ch_records <- record
				line_count++
				if line_count%1024 == 0 {
					log.Printf("line: %d from file %s was submitted", line_count, f)
				}
			}
		}
		fp.Close()
	}

	close(ch_records)
	wg.Wait()
}

func (self *Csv2KV) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response) chan *Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, 1000)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
