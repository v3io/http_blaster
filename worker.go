package main

import (
	"bufio"
	"crypto/tls"
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"os"
	"sync"
	"time"
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
	codes map[int]int
}

func (self *worker_load) Prepare_request(content_type string,
	header_args map[string]string, method string, uri string, body string) {

	self.req = &fasthttp.Request{}
	header := fasthttp.RequestHeader{}
	header.SetContentType(content_type)

	header.SetMethod(method)
	header.SetRequestURI(uri)

	for k, v := range header_args {
		header.Set(k, v)
	}
	self.req.AppendBodyString(body)
	header.CopyTo(&self.req.Header)
}

type worker struct {
	host                string
	conn                net.Conn
	br                  *bufio.Reader
	bw                  *bufio.Writer
	results             worker_results
	connection_restarts uint32
	is_tls_client       bool
}

func (w *worker) send_request(req *fasthttp.Request, resp *fasthttp.Response) (error, time.Duration) {
	err, duration := w.send(req, resp)
	if err != nil || resp.ConnectionClose() {
		w.restart_connection()
	}

	return err, duration
}

func (w *worker) open_connection() {
	conn, err := fasthttp.DialTimeout(w.host, DialTimeout)
	if err != nil {
		log.Printf("open connection error: %s\n", err)
		os.Exit(1)
	}
	if w.is_tls_client {
		conf := &tls.Config{
			InsecureSkipVerify: true,
		}
		w.conn = tls.Client(conn, conf)
	} else {
		w.conn = conn
	}
	w.br = bufio.NewReaderSize(w.conn, 16*1024)
	w.bw = bufio.NewWriter(w.conn)
}

func (w *worker) close_connection() {
	w.conn.Close()
}

func (w *worker) restart_connection() {
	w.close_connection()
	w.open_connection()
	w.connection_restarts++
}

func (w *worker) send(req *fasthttp.Request, resp *fasthttp.Response) (error, time.Duration) {
	start := time.Now()
	if err := req.Write(w.bw); err != nil {
		log.Printf("send write error: %s\n", err)
		return err, 0
	}
	if err := w.bw.Flush(); err != nil {
		log.Printf("send flush error: %s\n", err)
		return err, 0
	}
	if err := resp.Read(w.br); err != nil {
		log.Printf("send read error: %s\n", err)
		return err, 0
	}
	end := time.Now()
	duration := end.Sub(start)
	return nil, duration
}

func (w *worker) run_worker(load *worker_load, wg *sync.WaitGroup) {
	defer wg.Done()
	r := clone_request(load.req)
	w.results.min = time.Duration(10 * time.Second)
	resp := fasthttp.Response{}
	done := make(chan struct{})

	go func() {
		select {
		case <-time.After(load.duration.Duration):
			close(done)
		}
	}()

WLoop:
	for {
		select {
		case <-done:
			break WLoop
		default:
			if w.results.count < load.req_count {
				var (
					code int
				)
				err, duration := w.send_request(r, &resp)
				if err == nil {
					code = resp.StatusCode()
					w.results.codes[code]++
				}
				w.results.count++
				if duration < w.results.min {
					w.results.min = duration
				}
				if duration > w.results.max {
					w.results.max = duration
				}
				w.results.avg = w.results.avg + (duration-w.results.avg)/time.Duration(w.results.count)
			} else {
				break WLoop
			}
		}
	}
}

func NewWorker(host string, tls_client bool) *worker {
	if host == "" {
		return nil
	}
	worker := worker{host: host, is_tls_client: tls_client}
	worker.results.codes = make(map[int]int)
	worker.open_connection()
	return &worker
}
