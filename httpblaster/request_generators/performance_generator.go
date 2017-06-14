package request_generators

import (
	"bytes"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
)

type PerformanceGenerator struct {
	workload config.Workload
	base_uri string
	RequestCommon
}

func (self *PerformanceGenerator) UseCommon(c RequestCommon) {

}

func (self *PerformanceGenerator) GenerateRequests(wl config.Workload, tls_mode bool, host string) chan *Request {
	self.workload = wl
	if tls_mode {
		self.base_uri = fmt.Sprintf("https://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	} else {
		self.base_uri = fmt.Sprintf("http://%s/%s/%s", host, self.workload.Bucket, self.workload.Target)
	}
	var contentType string = "text/html"
	var payload []byte
	var Data_bfr []byte
	var ferr error
	if self.workload.Payload != "" {
		payload, ferr = ioutil.ReadFile(self.workload.Payload)
		if ferr != nil {
			log.Fatal(ferr)
		}
	} else {
		if self.workload.Type == http.MethodPut || self.workload.Type == http.MethodPost {
			payload = bytes.NewBuffer(Data_bfr).Bytes()

		}
	}
	req:=AcquireRequest()
	self.PrepareRequest(contentType, self.workload.Header, string(self.workload.Type),
		self.base_uri, string(payload), host, req.Request)
	ch_req := make(chan *Request, 1000)
	go func() {
		if self.workload.FileIndex == 0 && self.workload.FilesCount == 0 {
			self.single_file_submitter(ch_req, req.Request)
		} else {
			self.multi_file_submitter(ch_req, req.Request)
		}
	}()
	return ch_req
}

func (self *PerformanceGenerator) clone_request(req *fasthttp.Request) *Request {
	new_req := AcquireRequest()

	//new_req := fasthttp.AcquireRequest()
	req.Header.CopyTo(&new_req.Request.Header)
	new_req.Request.AppendBody(req.Body())
	return new_req
}

func (self *PerformanceGenerator) single_file_submitter(ch_req chan *Request, req *fasthttp.Request) {
	request := self.clone_request(req)
	for i := 0; i < self.workload.Count; i++ {
		ch_req <- request
	}
	close(ch_req)
}

func (self *PerformanceGenerator) gen_files_uri(file_index int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", self.base_uri, n+file_index)
			}
		} else {
			file_pref := file_index
			for {
				if file_pref == file_index+count {
					file_pref = file_index
				}
				ch <- fmt.Sprintf("%s_%d", self.base_uri, file_pref)
				file_pref += 1
			}
		}
	}()
	return ch
}

func (self *PerformanceGenerator) multi_file_submitter(ch_req chan *Request, req *fasthttp.Request) {
	ch_uri := self.gen_files_uri(self.workload.FileIndex, self.workload.Count, self.workload.Random)
	for i := 0; i < self.workload.Count; i++ {
		uri := <-ch_uri
		request := self.clone_request(req)
		request.Request.SetRequestURI(uri)
		ch_req <- request
	}
	close(ch_req)
}
