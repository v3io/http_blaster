package request_generators

import (
	"bufio"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"github.com/valyala/fasthttp"
	"hash/fnv"
	"io"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"strings"
	"sync"
)

type CSV2StreamGenerator struct {
	RequestCommon
	workload config.Workload
}

func (self *CSV2StreamGenerator) UseCommon(c RequestCommon) {

}

func (self *CSV2StreamGenerator) Hash32(line string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(line))
	return h.Sum32()
}

func (self *CSV2StreamGenerator) generate_request(ch_records chan string,
	ch_req chan *fasthttp.Request,
	host string, wg *sync.WaitGroup) {
	defer wg.Done()
	u, _ := uuid.NewV4()
	for r := range ch_records {
		columns := strings.Split(r, self.workload.Separator)
		shard_id := self.Hash32(columns[self.workload.ShardColumn]) % self.workload.ShardCount
		sr := igz_data.NewStreamRecord("client", r, u.String(), int(shard_id), true)
		r := igz_data.NewStreamRecords(sr)
		req := self.PrepareRequest(contentType, self.workload.Header, "PUT",
			self.base_uri, r.ToJsonString(), host)
		ch_req <- req
	}
	log.Println("generate_request Done")
}

func (self *CSV2StreamGenerator) generate(ch_req chan *fasthttp.Request, payload string, host string) {
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
				line, err := reader.ReadString('\n')
				if err == nil {
					ch_records <- strings.TrimSpace(line)
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

func (self *CSV2StreamGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, worker_qd int) chan *fasthttp.Request {
	self.workload = wl
	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.workload.Header["X-v3io-function"] = "PutRecords"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	ch_req := make(chan *fasthttp.Request, worker_qd)

	go self.generate(ch_req, self.workload.Payload, host)

	return ch_req
}
