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
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster/request_generators"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
	"net/http"
)

const DialTimeout = 60 * time.Second
const RequestTimeout = 600 * time.Second

var once sync.Once
var dump_dir string

type worker_results struct {
	count  uint64
	min    time.Duration
	max    time.Duration
	avg    time.Duration
	read   uint64
	write  uint64
	codes  map[int]uint64
	method string
}

type worker struct {
	host                string
	conn                net.Conn
	results             worker_results
	connection_restarts uint32
	error_count         uint32
	is_tls_client       bool
	pem_file            string
	br                  *bufio.Reader
	bw                  *bufio.Writer
	ch_duration         chan time.Duration
	ch_error            chan error
	lazy_sleep          time.Duration
	retry_codes         map[int]interface{}
	retry_count         int
	timer               *time.Timer
	id                  int
}

func (w *worker) send_request(req *request_generators.Request) (error, time.Duration, *request_generators.Response) {
	response := request_generators.AcquireResponse()
	var (
		code     int
		err      error
		duration time.Duration
	)
	if w.lazy_sleep > 0 {
		time.Sleep(w.lazy_sleep)
	}

	err, duration = w.send(req.Request, response.Response, RequestTimeout)

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
		w.results.avg = w.results.avg + (duration-w.results.avg)/time.Duration(w.results.count)
	} else {
		w.error_count++
		log.Debugln(err.Error())

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
	} else {
		w.conn = conn
	}
	w.br = bufio.NewReader(w.conn)
	if w.br == nil {
		log.Errorf("Reader is nil, conn: %v", conn)
	}
	w.bw = bufio.NewWriter(w.conn)
}

func (w *worker) open_secure_connection(conn net.Conn) *tls.Conn {
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
		block, _ := pem.Decode([]byte(pem_data))
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			panic(err)
			log.Fatal(err)
		}
		clientCertPool := x509.NewCertPool()
		clientCertPool.AddCert(cert)

		conf = &tls.Config{
			ServerName:         w.host,
			ClientAuth:         tls.RequireAndVerifyClientCert,
			InsecureSkipVerify: true,
			ClientCAs:          clientCertPool,
		}
	} else {
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
			log.Debugf("send write error: %s\n", err)
			log.Debugln(fmt.Sprintf("%+v", req))
			w.ch_error <- err
			return
		} else if err = w.bw.Flush(); err != nil {
			log.Debugf("send flush error: %s\n", err)
			w.ch_error <- err
			return
		} else if err = resp.Read(w.br); err != nil {
			log.Debugf("send read error: %s\n", err)
			w.ch_error <- err
			return
		}
		end := time.Now()
		w.ch_duration <- end.Sub(start)
	}()

	w.timer.Reset(timeout)
	select {
	case duration := <-w.ch_duration:
		return nil, duration
	case err := <-w.ch_error:
		log.Debugf("request completed with error:%s", err.Error())
		return err, timeout
	case <-w.timer.C:
		log.Printf("Error: request didn't complete on timeout url:%s", req.URI().String())
		return errors.New(fmt.Sprintf("request timedout url:%s", req.URI().String())), timeout
	}
	return nil, timeout
}

func (w *worker) dump_requests(ch_dump chan *fasthttp.Request, dump_location string,
	sync_dump *sync.WaitGroup) {

	once.Do(func() {
		t := time.Now()
		dump_dir = path.Join(dump_location, fmt.Sprintf("BlasterDump-%v", t.Format("2006-01-02-150405")))
		err := os.Mkdir(dump_dir, 0777)
		if err != nil {
			log.Errorf("Fail to create dump dir %v:%v", dump_dir, err.Error())
		}
	})
	defer sync_dump.Done()

	i := 0
	for r := range ch_dump {
		file_name := fmt.Sprintf("w%v_request_%v", w.id, i)
		file_path := filepath.Join(dump_dir, file_name)
		log.Info("generating dump file ", file_path)
		i++
		file, err := os.Create(file_path)
		if err != nil {
			log.Errorf("Fail to open file %v for request dump: %v", file_path, err.Error())
		} else {
			rdump := &request_generators.RequestDump{}
			rdump.Host = string(r.Host())
			rdump.Method = string(r.Header.Method())
			rdump.Body = string(r.Body())
			rdump.URI = r.URI().String()
			rdump.Headers = make(map[string]string)
			r.Header.VisitAll(func(key, value []byte) {
				rdump.Headers[string(key)] = string(value)
			})
			jsonStr, err := json.Marshal(rdump)
			if err != nil {
				log.Errorf("Fail to dump request %v", err.Error())
			}
			log.Debug("Write dump request")
			file.Write(jsonStr)
			file.Close()
		}
	}
}

func (w *worker) run_worker(ch_resp chan *request_generators.Response, ch_req chan *request_generators.Request,
	wg *sync.WaitGroup, release_req bool,
	ch_latency chan time.Duration,
	ch_statuses chan int,
	dump_requests bool,
	dump_location string) {
	defer wg.Done()
	var onceSetRequest sync.Once
	var oncePrepare sync.Once
	var request *request_generators.Request
	submit_request := request_generators.AcquireRequest()
	var req_type sync.Once
	var ch_dump chan *fasthttp.Request
	var sync_dump sync.WaitGroup
	if dump_requests {
		ch_dump = make(chan *fasthttp.Request, 100)
		sync_dump.Add(1)
		go w.dump_requests(ch_dump, dump_location, &sync_dump)
	}

	prepareRequest := func() {
		request.Request.Header.CopyTo(&submit_request.Request.Header)
		submit_request.Request.AppendBody(request.Request.Body())
		submit_request.Request.SetHost(w.host)
	}

	for req := range ch_req {
		req_type.Do(func() {
			w.results.method = string(req.Request.Header.Method())
		})

		if release_req {
			req.Request.SetHost(w.host)
			submit_request = req
		} else {
			onceSetRequest.Do(func() {
				request = req
			})
			oncePrepare.Do(prepareRequest)
		}

		var response *request_generators.Response
		var err error
		var d time.Duration
	LOOP:
		for i := 0; i < w.retry_count; i++ {
			err, d, response = w.send_request(submit_request)
			if err != nil {
				//retry on error
				request_generators.ReleaseResponse(response)
				continue
			} else if response.Response.StatusCode() >= http.StatusBadRequest {
				if _, ok := w.retry_codes[response.Response.StatusCode()]; !ok {
					//not subject to retry
					ch_statuses <- response.Response.StatusCode()
					ch_latency <- d
					break LOOP
				} else if i+1 < w.retry_count {
					//not the last loop
					request_generators.ReleaseResponse(response)
				}
				ch_statuses <- response.Response.StatusCode()
				ch_latency <- d
			} else{
				break LOOP
			}
		}
		ch_statuses <- response.Response.StatusCode()
		ch_latency <- d
		if response.Response.StatusCode() >= http.StatusBadRequest &&
			response.Response.StatusCode() < http.StatusInternalServerError &&
				dump_requests {
			//dump request
			r := fasthttp.AcquireRequest()
			r.SetBody(submit_request.Request.Body())
			submit_request.Request.CopyTo(r)
			ch_dump <- r
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
	if dump_requests {
		log.Info("wait for dump routine to end")
		close(ch_dump)
		sync_dump.Wait()
	}
	w.close_connection()
}

func NewWorker(host string, tls_client bool, lazy int, retry_codes []int, retry_count int, pem_file string, id int) *worker {
	if host == "" {
		return nil
	}
	retry_codes_map := make(map[int]interface{})
	for _, c := range retry_codes {
		retry_codes_map[c] = true

	}
	if retry_count == 0 {
		retry_count = 1
	}
	worker := worker{host: host, is_tls_client: tls_client, retry_codes: retry_codes_map,
		retry_count: retry_count, pem_file: pem_file, id: id}
	worker.results.codes = make(map[int]uint64)
	worker.results.min = time.Duration(time.Second * 10)
	worker.open_connection()
	worker.ch_duration = make(chan time.Duration, 1)
	worker.ch_error = make(chan error, 1)
	worker.lazy_sleep = time.Duration(lazy) * time.Millisecond
	worker.timer = time.NewTimer(time.Second * 120)
	return &worker
}
