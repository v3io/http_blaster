/*
Copyright 2016 Iguazio.io Systems Ltd.

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
package httpblaster

import (
	"crypto/tls"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
	//"os"
	"bufio"
	"errors"
)

const DialTimeout = 60 * time.Second

type worker_load struct {
	req       *fasthttp.Request
	req_count uint64
	duration  duration
	port      string
}

type worker_results struct {
	count uint64
	min   time.Duration
	max   time.Duration
	avg   time.Duration
	read  uint64
	write uint64
	codes map[int]uint64
}

func (self *worker_load) Prepare_request(content_type string,
	header_args map[string]string, method string, uri string, body string, host string) {

	self.req = &fasthttp.Request{}
	header := fasthttp.RequestHeader{}
	header.SetContentType(content_type)

	header.SetMethod(method)
	header.SetRequestURI(uri)
	header.SetHost(host)

	for k, v := range header_args {
		header.Set(k, v)
	}
	self.req.AppendBodyString(body)
	header.CopyTo(&self.req.Header)
}

type worker struct {
	host                string
	conn                net.Conn
	results             worker_results
	connection_restarts uint32
	error_count         uint32
	is_tls_client       bool
	base_uri            string
	br                  *bufio.Reader
	bw                  *bufio.Writer
	ch_duration         chan time.Duration
	ch_error            chan error
}

func (w *worker) send_request(req *fasthttp.Request) (error, time.Duration) {
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)

	var (
		code int
	)
	err, duration := w.send(req, response, time.Second*60)

	if err != nil || response.ConnectionClose() {
		w.restart_connection()
		if err != nil {
			log.Println("[ERROR]", err.Error())
		} else {
			log.Println(fmt.Sprintf("Connection close, response status code %d", response.StatusCode()))
		}
	}
	if err == nil {
		code = response.StatusCode()
		w.results.codes[code]++

		w.results.count++
		if duration < w.results.min {
			w.results.min = duration
		}
		if duration > w.results.max {
			w.results.max = duration
		}
		w.results.avg = w.results.avg + (duration-w.results.avg)/time.Duration(w.results.count)
	} else {
		w.error_count++
	}

	return err, duration
}

func (w *worker) open_connection() {
	conn, err := fasthttp.DialTimeout(w.host, DialTimeout)
	if err != nil {
		log.Printf("open connection error: %s\n", err)
	}
	if w.is_tls_client {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		w.conn = tls.Client(conn, conf)
	} else {
		w.conn = conn
	}
	w.br = bufio.NewReaderSize(w.conn, 1024*1024)
	w.bw = bufio.NewWriter(w.conn)
}

func (w *worker) close_connection() {
	if w.conn != nil {
		w.conn.Close()
	}
}

func (w *worker) restart_connection() {
	w.close_connection()
	w.open_connection()
	w.connection_restarts++
}

func (w *worker) send(req *fasthttp.Request, resp *fasthttp.Response,
	timeout time.Duration) (error, time.Duration) {
	var err error
	go func() {
		start := time.Now()
		if err = req.Write(w.bw); err != nil {
			log.Printf("send write error: %s\n", err)
			w.ch_error <- err
		}
		if err = w.bw.Flush(); err != nil {
			log.Printf("send flush error: %s\n", err)
			w.ch_error <- err
		}
		if err = resp.Read(w.br); err != nil {
			log.Printf("send read error: %s\n", err)
			w.ch_error <- err
		}
		end := time.Now()
		w.ch_duration <- end.Sub(start)
	}()
	select {
	case duration := <-w.ch_duration:
		return nil, duration
	case err := <-w.ch_error:
		log.Printf("rerquest completed with error:%s url:%s", err.Error(), req.URI().String())
		return err, timeout
	case <-time.After(timeout):
		log.Printf("Error: request didn't complete on timeout url:%s", req.URI().String())
		return errors.New(fmt.Sprintf("request timedout url:%s", req.URI().String())), timeout
	}
	return nil, timeout
}

func (w *worker) gen_files_uri(file_index int, count int, random bool) chan string {
	ch := make(chan string, 1000)
	go func() {
		if random {
			for {
				n := rand.Intn(count)
				ch <- fmt.Sprintf("%s_%d", w.base_uri, n+file_index)
			}
		} else {
			file_pref := file_index
			for {
				if file_pref == file_index+count {
					file_pref = file_index
				}
				ch <- fmt.Sprintf("%s_%d", w.base_uri, file_pref)
				file_pref += 1
			}
		}
	}()
	return ch
}

func (w *worker) single_file_submitter(done chan struct{}, load *worker_load) {
	request := clone_request(load.req)
LOOP:
	for {
		select {
		case <-done:
			break LOOP
		default:
			if w.results.count < load.req_count {
				w.send_request(request)
			} else {
				break LOOP
			}
		}
	}
}

func (w *worker) multi_file_submitter(done chan struct{}, load *worker_load, file_index int, count int, random bool) {
	ch_uri := w.gen_files_uri(file_index, count, random)
	request := clone_request(load.req)
WLoop:
	for {
		select {
		case <-done:
			break WLoop
		case uri := <-ch_uri:
			if w.results.count < load.req_count {
				request.SetRequestURI(uri)
				w.send_request(request)
			} else {
				break WLoop
			}
		}
	}
}

func (w *worker) run_worker(load *worker_load, wg *sync.WaitGroup, file_index int, count int, random bool) {
	defer wg.Done()
	w.results.min = time.Duration(10 * time.Second)
	done := make(chan struct{})

	go func() {
		select {
		case <-time.After(load.duration.Duration):
			close(done)
		}
	}()
	if file_index == 0 && count == 0 {
		w.single_file_submitter(done, load)
	} else {
		w.multi_file_submitter(done, load, file_index, count, random)
	}
}

func NewWorker(host string, tls_client bool, base_uri string) *worker {
	if host == "" {
		return nil
	}
	worker := worker{host: host, is_tls_client: tls_client, base_uri: base_uri}
	worker.results.codes = make(map[int]uint64)
	worker.open_connection()
	worker.ch_duration = make(chan time.Duration, 1)
	worker.ch_error = make(chan error, 1)
	return &worker
}
