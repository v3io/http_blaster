package request_generators

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/config"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
)

type RestoreGenerator struct {
	RequestCommon
	workload         config.Workload
	re_item          *regexp.Regexp
	re_items         *regexp.Regexp
	re_name          *regexp.Regexp
	re_collection_id *regexp.Regexp
	re_remove_items  *regexp.Regexp
	emd_ignore_attrs []string
}

type BackupItem struct {
	Payload []byte
	Uri     string
}

func (self *RestoreGenerator) UseCommon(c RequestCommon) {

}

func (self *RestoreGenerator) LoadSchema(file_path string) (error, map[string]interface{}) {
	type backup_schema struct {
		records map[interface{}]interface{}
		inode   map[interface{}]interface{}
		shards  []interface{}
		dir     map[interface{}]map[interface{}]interface{}
	}

	plan, _ := ioutil.ReadFile(file_path)

	var data interface{}
	err := jsoniter.Unmarshal(plan, &data)

	if err != nil {
		panic(err)
	}
	if val, ok := data.(map[string]interface{})["inode"]; ok {
		return nil, val.(map[string]interface{})
	}
	return errors.New("fail to get inode table"), nil

}

type items_s struct {
	LastItemIncluded interface{}
	NextKey          string
	EvaluatedItems   int
	NumItems         int
	NextMarker       string
	Items            []map[string]map[string]interface{}
}

func (self *RestoreGenerator) generate_items(ch_lines chan []byte, collection_ids map[string]interface{}) chan *BackupItem {
	ch_items := make(chan *BackupItem, 100000)
	wg := sync.WaitGroup{}
	routines := 1 //runtime.NumCPU()/2
	wg.Add(routines)
	go func() {
		for i := 0; i < routines; i++ {
			go func() {
				defer wg.Done()
				for line := range ch_lines {
					var items_j items_s
					err := jsoniter.Unmarshal(line, &items_j)
					if err != nil {
						log.Println("Unable to Unmarshal line:", string(line))
						panic(err)
					}
					items := items_j.Items
					for _, i := range items {
						item_name := i["__name"]["S"]
						collection_id := i["__collection_id"]["N"]
						dir_name := collection_ids[collection_id.(string)]
						if dir_name == nil {
							log.Errorf("Fail to get dir name for collection id: %v", collection_id)
							continue
						}
						for _, attr := range self.emd_ignore_attrs {
							delete(i, attr)
						}

						j, e := jsoniter.Marshal(i)
						if e != nil {
							log.Println("Unable to Marshal json:", i)
							panic(e)
						}
						var payload bytes.Buffer
						if len(i) != 0 {
							payload.WriteString(`{"Item": `)
							payload.Write(j)
							payload.WriteString(`}`)
							ch_items <- &BackupItem{Uri: self.base_uri + dir_name.(string) + item_name.(string),
								Payload: payload.Bytes()}
						}
					}
				}
			}()
		}
		wg.Wait()
		close(ch_items)
	}()
	return ch_items
}

func (self *RestoreGenerator) generate(ch_req chan *Request,
	ch_items chan *BackupItem, host string) {
	defer close(ch_req)
	wg := sync.WaitGroup{}

	routines := 1 //runtime.NumCPU()
	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			for item := range ch_items {
				req := AcquireRequest()
				self.PrepareRequestBytes(contentType, self.workload.Header, "PUT",
					item.Uri, item.Payload, host, req.Request)
				ch_req <- req
			}
		}()
	}
	log.Println("Waiting for generators to finish")
	wg.Wait()
	log.Println("generators done")
}

func (self *RestoreGenerator) line_reader() chan []byte {
	ch_lines := make(chan []byte, 24)
	ch_files := self.FilesScan(self.workload.Payload)
	go func() {
		for f := range ch_files {
			if file, err := os.Open(f); err == nil {
				reader := bufio.NewReader(file)
				var i int = 0
				for {
					line, line_err := reader.ReadBytes('\n')
					if line_err == nil {
						ch_lines <- line
						i++
					} else if line_err == io.EOF {
						break
					} else {
						log.Fatal(err)
					}
				}

				log.Println(fmt.Sprintf("Finish file scaning %v, generated %d records", f, i))
			} else {
				panic(err)
			}
		}
		close(ch_lines)
	}()
	log.Println("finish line generation")
	return ch_lines
}

func (self *RestoreGenerator) GenerateRequests(global config.Global, wl config.Workload, tls_mode bool, host string, ret_ch chan *Response, worker_qd int) chan *Request {
	self.workload = wl
	ch_req := make(chan *Request, worker_qd)

	if self.workload.Header == nil {
		self.workload.Header = make(map[string]string)
	}
	self.emd_ignore_attrs = global.IgnoreAttrs

	self.workload.Header["X-v3io-function"] = "PutItem"

	self.SetBaseUri(tls_mode, host, self.workload.Container, self.workload.Target)

	err, inode_map := self.LoadSchema(wl.Schema)

	if err != nil {
		panic(err)
	}

	ch_lines := self.line_reader()

	ch_items := self.generate_items(ch_lines, inode_map)

	go self.generate(ch_req, ch_items, host)

	return ch_req
}
