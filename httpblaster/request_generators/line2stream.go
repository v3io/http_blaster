package request_generators

import (
	"bufio"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
)

type Line2StreamGenerator struct {
	RequestCommon
	workload config.Workload
	base_uri string
}

func (self *Line2StreamGenerator) UseCommon(c RequestCommon) {

}

func (self *Line2StreamGenerator) generate_request(ch_records chan string,
	ch_req chan Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	var contentType string = "application/json"
	u, _ := uuid.NewV4()
	for r := range ch_records {
		sr := igz_data.NewStreamRecord("client", r, u.String(), 0, true)
		r := igz_data.NewStreamRecords(sr)
		req := self.PrepareRequest(contentType, self.workload.Header, self.workload.Type,
			self.base_uri, r.ToJsonString(), host)
		ch_req <- Request{Request: req}
	}
	log.Println("generate_request Done")
}

func (self *Line2StreamGenerator) generate(ch_req chan Request, payload string, host string) {
	defer close(ch_req)
	var ch_records chan string = make(chan string)
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
				line, _, err := reader.ReadLine()
				if err == nil {
					ch_records <- string(line)
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
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *Line2StreamGenerator) GenerateRequests(wl config.Workload, tls_mode bool, host string) chan Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutRecords"

	if tls_mode {
		self.base_uri = fmt.Sprintf("https://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	} else {
		self.base_uri = fmt.Sprintf("http://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	}
	ch_req := make(chan Request, 1000)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
