/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package request_generators

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"net/url"
	"os"
	"path/filepath"
)

const (
	PERFORMANCE = "performance"
	LINE2STREAM = "line2stream"
	CSV2KV      = "csv2kv"
	CSVUPDATEKV = "csvupdatekv"
	CSV2STREAM  = "csv2stream"
	JSON2KV     = "json2kv"
	STREAM_GET  = "stream_get"
	LINE2KV     = "line2kv"
	RESTORE     = "restore"
	LINE2HTTP   = "line2http"
	REPLAY      = "replay"
	CSV2TSDB    = "csv2tsdb"
	STATS2TSDB  = "stats2tsdb"
	FAKER2KV    = "faker2kv"
	CSV2KVPERF  = "csv2kv_perf"
)

type RequestCommon struct {
	ch_files chan string
	base_uri string
}

var (
	contentType string = "application/json"
)

func (self *RequestCommon) PrepareRequest(content_type string,
	header_args map[string]string,
	method string, uri string,
	body string, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(content_type)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range header_args {
		req.Header.Set(k, v)
	}
	req.AppendBodyString(body)
}

func (self *RequestCommon) PrepareRequestBytes(content_type string,
	header_args map[string]string,
	method string, uri string,
	body []byte, host string, req *fasthttp.Request) {
	u := url.URL{Path: uri}
	req.Header.SetContentType(content_type)
	req.Header.SetMethod(method)
	req.Header.SetRequestURI(u.EscapedPath())
	req.Header.SetHost(host)
	for k, v := range header_args {
		req.Header.Set(k, v)
	}
	req.AppendBody(body)
}

func (self *RequestCommon) SubmitFiles(path string, info os.FileInfo, err error) error {
	log.Print(path)
	if err != nil {
		log.Print(err)
		return nil
	}
	if !info.IsDir() {
		self.ch_files <- path
	}
	fmt.Println(path)
	return nil
}

func (self *RequestCommon) FilesScan(path string) chan string {
	self.ch_files = make(chan string)
	go func() {
		err := filepath.Walk(path, self.SubmitFiles)
		if err != nil {
			log.Fatal(err)
		}
		close(self.ch_files)
	}()
	return self.ch_files
}

func (self *RequestCommon) SetBaseUri(tls_mode bool, host string, container string, target string) {
	http := "http"
	if tls_mode {
		http += "s"
	}
	self.base_uri = fmt.Sprintf("%s://%s/%s/%s", http, host, container, target)
}
