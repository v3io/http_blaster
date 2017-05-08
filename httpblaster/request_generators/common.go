package request_generators

import (
	"github.com/valyala/fasthttp"
	"os"
	"log"
	"fmt"
	"path/filepath"
	"time"
)

const (
	PERFORMANCE = "performance"
	CSV2STREAM  = "csv2stream"
	CSV2KV      = "csv2kv"
)

type RequestCommon struct {
	ch_files chan string
}

func (self *RequestCommon) PrepareRequest(content_type string,
	header_args map[string]string,
	method string, uri string,
	body string, host string) *fasthttp.Request {
	req := fasthttp.AcquireRequest()

	header := fasthttp.RequestHeader{}
	header.SetContentType(content_type)

	header.SetMethod(method)
	header.SetRequestURI(uri)
	header.SetHost(host)

	for k, v := range header_args {
		header.Set(k, v)
	}
	req.AppendBodyString(body)
	header.CopyTo(&req.Header)
	return req
}



func (self *RequestCommon)SubmitFiles(path string, info os.FileInfo, err error) error {
	log.Print(path)
	if err != nil {
		log.Print(err)
		return nil
	}
	if !info.IsDir(){
		self.ch_files <- path
	}
	fmt.Println(path)
	return nil
}

func (self *RequestCommon) FilesScan(path string) chan string{
	self.ch_files = make(chan string)
	go func() {
		err := filepath.Walk(path, self.SubmitFiles)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second*3)
		close(self.ch_files)
	}()
	return self.ch_files
}