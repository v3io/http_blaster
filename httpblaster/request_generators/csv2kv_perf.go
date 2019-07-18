package request_generators

import (
	"encoding/csv"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Csv2KVPerf struct {
	workload config.Workload
	RequestCommon
}

func (self *Csv2KVPerf) UseCommon(c RequestCommon) {

}

func (self *Csv2KVPerf) generate_request(ch_records chan []string, ch_req chan *Request, host string,
	wg *sync.WaitGroup, done chan struct{}) {
	defer wg.Done()
	parser := igz_data.EmdSchemaParser{}
	var contentType string = "text/html"
	e := parser.LoadSchema(self.workload.Schema, "", "")
	if e != nil {
		panic(e)
	}
	for {
		select {
		case <-done:
			return
		case r := <-ch_records:
			json_payload := parser.EmdFromCSVRecord(r)
			req := AcquireRequest()
			self.PrepareRequest(contentType, self.workload.Header, "PUT",
				self.base_uri, json_payload, host, req.Request)
			ch_req <- req
		}
	}
}

func (self *Csv2KVPerf) generate(ch_req chan *Request, payload string, host string, done chan struct{}) {
	defer close(ch_req)

	ch_records:= self.generate_records(payload, host, done)

	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg, done)
	}

	wg.Wait()
}

func (self *Csv2KVPerf)generate_records(payload string, host string, done chan struct{})  (chan []string) {
	var ch_records chan []string = make(chan []string, 1000)

	go func(){
		parser := igz_data.EmdSchemaParser{}
		e := parser.LoadSchema(self.workload.Schema, "", "")
		if e != nil {
			panic(e)
		}
		for {
			ch_files := self.FilesScan(self.workload.Payload)
			for f := range ch_files {
				log.Info("Scaning file ",f," for records")
				fp, err := os.Open(f)
				if err != nil {
					panic(err)
				}

				r := csv.NewReader(fp)
				r.Comma = parser.JsonSchema.Settings.Separator.Rune
				var line_count= 0
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
					select {
					case <-done:
						log.Info("stopping")
						close(ch_records)
						return
					default:
					}

				}
				fp.Close()
			}

			//close(ch_records)
		}
	}()
	return ch_records
}



func (self *Csv2KVPerf) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *Request, worker_qd)

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(self.workload.Duration.Duration):
			log.Info("Time has come")
			close(done)
		}
	}()


	go self.generate(ch_req, self.workload.Payload, host, done)

	return ch_req
}
