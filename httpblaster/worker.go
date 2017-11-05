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
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"sync"
	"time"
	"encoding/pem"
	"crypto/x509"
	"os"
	"io/ioutil"
)

const DialTimeout = 60 * time.Second

type worker_results struct {
	count uint64
	min   time.Duration
	max   time.Duration
	avg   time.Duration
	read  uint64
	write uint64
	codes map[int]uint64
	method string
}

type worker struct {
	host                string
	conn                net.Conn
	results             worker_results
	connection_restarts uint32
	error_count         uint32
	is_tls_client       bool
	pem_file	    string
	br                  *bufio.Reader
	bw                  *bufio.Writer
	ch_duration         chan time.Duration
	ch_error            chan error
	lazy_sleep          time.Duration
	retry_codes 	    map[int]interface{}
	retry_count 	    int
	timer 		    *time.Timer
}


func (w *worker) send_request(req *request_generators.Request) (error, time.Duration, *request_generators.Response) {
	response := request_generators.AcquireResponse()
	var (
		code int
		err error
		duration time.Duration
	)
	if w.lazy_sleep > 0 {
		time.Sleep(w.lazy_sleep)
	}

	err, duration = w.send(req.Request, response.Response, time.Second * 60)

	if err == nil {
		code = response.Response.StatusCode()
		w.results.codes[code]++

		w.results.count++
		if duration < w.results.min {
			w.results.min = duration
		}
		if duration > w.results.max {
			w.results.max = duration
		}
		w.results.avg = w.results.avg + (duration - w.results.avg) / time.Duration(w.results.count)
	} else {
		w.error_count++
		log.Println("[ERROR]", err.Error())

	}
	if response.Response.ConnectionClose() {
		w.restart_connection()
	}

	return err, duration, response
}

func (w *worker) open_connection() {
	conn, err := fasthttp.DialTimeout(w.host, DialTimeout)
	if err != nil {
		panic(err)
		log.Printf("open connection error: %s\n", err)
	}
	if w.is_tls_client {
		w.conn = w.open_secure_connection(conn)
	}else{
		w.conn = conn
	}
	w.br = bufio.NewReaderSize(w.conn, 1024*1024)
	w.bw = bufio.NewWriter(w.conn)
}

func (w *worker) open_secure_connection(conn net.Conn) *tls.Conn{
	var conf *tls.Config
	if w.pem_file != "" {
		var pem_data []byte
		fp, err := os.Open(w.pem_file)
		if err != nil {
			panic(err)
		} else {
			defer fp.Close()
			pem_data, err = ioutil.ReadAll(fp)
			if err != nil {
				panic(err)
			}
		}
		block, _ := pem.Decode([]byte (pem_data))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			panic(err)
			log.Fatal(err)
		}
		clientCertPool := x509.NewCertPool()
		clientCertPool.AddCert(cert)

		conf = &tls.Config{
			ServerName: w.host,
			ClientAuth: tls.RequireAndVerifyClientCert,
			InsecureSkipVerify: true,
			ClientCAs: clientCertPool,
		}
	}else{
		conf = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	c := tls.Client(conn, conf)
	return c
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
			log.Println(fmt.Sprintf("%+v", req))
			w.ch_error <- err
		} else if err = w.bw.Flush(); err != nil {
			log.Printf("send flush error: %s\n", err)
			w.ch_error <- err
		} else if err = resp.Read(w.br); err != nil {
			log.Printf("send read error: %s\n", err)
			w.ch_error <- err
		}
		end := time.Now()
		w.ch_duration <- end.Sub(start)
	}()
	w.timer.Reset(timeout)
	select {
	case duration := <-w.ch_duration:
		return nil, duration
	case err := <-w.ch_error:
		log.Printf("rerquest completed with error:%s", err.Error())
		return err, timeout
	case <- w.timer.C:
		log.Printf("Error: request didn't complete on timeout url:%s", req.URI().String())
		return errors.New(fmt.Sprintf("request timedout url:%s", req.URI().String())), timeout
	}
	return nil, timeout
}


func (w *worker) run_worker(ch_resp chan *request_generators.Response, ch_req chan *request_generators.Request,
				wg *sync.WaitGroup, release_req bool,
				ch_latency chan time.Duration,
				ch_statuses chan int) {
	defer wg.Done()
	var req_type sync.Once

	for req := range ch_req {
		req_type.Do(func() {
			w.results.method = string(req.Request.Header.Method())
		})
		var response *request_generators.Response
		var err error
		LOOP:
		for i := 0; i < w.retry_count; i++ {
			var d time.Duration
			err, d, response = w.send_request(req)
			if err != nil {
				//retry on error
				request_generators.ReleaseResponse(response)
				continue
			} else if _, ok := w.retry_codes[response.Response.StatusCode()]; !ok {
				//not subject to retry
				ch_statuses <- response.Response.StatusCode()
				ch_latency <- d
				break LOOP
			} else if i + 1 < w.retry_count {
				//not the last loop
				request_generators.ReleaseResponse(response)
			}
		}
		if ch_resp != nil {
			ch_resp <- response
		} else {
			request_generators.ReleaseResponse(response)
		}
		if release_req {
			request_generators.ReleaseRequest(req)
		}
	}
}

func NewWorker(host string, tls_client bool, lazy int, retry_codes []int, retury_count int, pem_file string) *worker {
	if host == "" {
		return nil
	}
	retry_codes_map := make(map[int]interface{})
	for _,c := range retry_codes{
		retry_codes_map[c]=true

	}
	if retury_count == 0 {
		retury_count = 1
	}
	worker := worker{host: host, is_tls_client: tls_client, retry_codes:retry_codes_map,
		retry_count:retury_count, pem_file:pem_file}
	worker.results.codes = make(map[int]uint64)
	worker.open_connection()
	worker.ch_duration = make(chan time.Duration, 1)
	worker.ch_error = make(chan error, 1)
	worker.lazy_sleep = time.Duration(lazy) * time.Millisecond
	worker.timer = time.NewTimer(time.Second * 120)
	return &worker
}
