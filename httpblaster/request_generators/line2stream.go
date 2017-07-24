package request_generators

import (
	"bufio"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"github.com/valyala/fasthttp"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
)

type Line2StreamGenerator struct {
	RequestCommon
	workload config.Workload
}

func (self *Line2StreamGenerator) UseCommon(c RequestCommon) {

}

func (self *Line2StreamGenerator) generate_request(ch_records chan string,
	ch_req chan *fasthttp.Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range ch_records {
		sr := igz_data.NewStreamRecord("client", r, u.String(), 0, true)
		r := igz_data.NewStreamRecords(sr)
		req := self.PrepareRequest(contentType, self.workload.Header, "PUT",
			self.base_uri, r.ToJsonString(), host)
		ch_req <- req
	}
	log.Println("generate_request Done")
}

func (self *Line2StreamGenerator) generate(ch_req chan *fasthttp.Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan string = make(chan string, 10000)
	wg := sync.WaitGroup{}
	ch_files := self.FilesScan(self.workload.Payload)

	wg.Add(runtime.NumCPU())
	for c := 0; c < runtime.NumCPU(); c++ {
		go self.generate_request(ch_records, ch_req, host, &wg)
	}

	for f := range ch_files {
		if file, err := os.Open(f); err == nil {
			reader := bufio.NewReader(file)
			var line_count int = 0
			for {
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_records <- strings.TrimSpace(line)
					line_count++
					if line_count % 1024 == 0{
						log.Printf("line: %d from file %s was submitted", line_count, f)
					}
				} else if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}

			log.Println(fmt.Sprintf("Finish file scaning, generated %d records", line_count))
		} else {
			panic(err)
		}
	}
	close(ch_records)
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *Line2StreamGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string) chan *fasthttp.Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutRecords"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *fasthttp.Request, 1000)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
