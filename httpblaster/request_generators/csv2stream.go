package request_generators

import (
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/valyala/fasthttp"
	"github.com/v3io/http_blaster/httpblaster/schema_parser"
	"runtime"
	"bufio"
	"os"
	"fmt"
	"dft/igz_data"
)



type Csv2StreamGenerator struct {
	RequestCommon
	workload config.Workload
	base_uri string
}

func (self *Csv2StreamGenerator) UseCommon(c RequestCommon) {

}

func (self *Csv2StreamGenerator)generate_request(ch_records chan string, ch_req chan *fasthttp.Request, host string)  {
	p := schema_parser.SchemaParser{}
	var contentType string = "text/html"
	e:=p.LoadSchema(self.workload.Schema)
	if e!= nil{
		panic(e)
	}
	for r := range ch_records{
		sr := igz_data.NewStreamRecord("client", r, "", 0)
		req := self.PrepareRequest(contentType, self.workload.Header, string(self.workload.Type),
			self.base_uri, sr.GetData(), host)
		ch_req<- req
	}
}

func (self *Csv2StreamGenerator)generate(ch_req chan *fasthttp.Request , payload string, host string){
	defer close(ch_req)
	var ch_records chan string = make(chan string)
	defer close(ch_records)

	if file, err := os.Open(self.workload.Payload); err == nil {
		scanner := bufio.NewScanner(file)

		for c := 0; c < runtime.NumCPU(); c++ {
			go self.generate_request(ch_records, ch_req, host)
		}

		for scanner.Scan() {
			ch_records <- scanner.Text()
		}
	}else{
		panic(err)
	}
}

func (self *Csv2StreamGenerator) GenerateRequests(wl config.Workload, tls_mode bool, host string) chan *fasthttp.Request {
	self.workload = wl
	self.workload.Header["X-v3io-function"]="PUtRecords"

	if tls_mode {
		self.base_uri = fmt.Sprintf("https://%s/%s/%s", host, self.workload.Bucket, self.workload.File_path)
	} else {
		self.base_uri = fmt.Sprintf("http://%s/%s/%s", host, self.workload.Bucket, self.workload.File_path)
	}
	ch_req := make(chan *fasthttp.Request, 1000)

	go self.generate(ch_req ,self.workload.Payload, host)

	return ch_req
}
