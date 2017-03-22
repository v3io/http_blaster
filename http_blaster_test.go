package httpblaster

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

//validate all conf files in the examples are valid.
func Test_Validate_Config_Examples(t *testing.T) {
	example_dir := "examples"
	files, err := ioutil.ReadDir(example_dir)
	if err != nil {
		t.Errorf(err.Error())
	}

	for _, file := range files {
		if !file.IsDir() {
			if file.Name() == "cfg_generator.sh"{
				continue
			}
			_, err := LoadConfig(path.Join(example_dir, file.Name()))
			if err != nil {
				t.Errorf("Failed to load conf %s", file.Name())
			}
		}
	}
}

func prepare_test_file(folder string, file string, bfr []byte) error {
	f, e := os.OpenFile(filepath.Join(folder, file), os.O_CREATE|os.O_RDWR, 0666)
	if e != nil {
		return e
	}
	f.Write(bfr)
	f.Close()
	return nil
}

func Test_GET_Worker(t *testing.T) {
	host := "127.0.0.1"
	port := "8080"
	method := "GET"
	file := "http_blaster.html"
	folder := "/tmp"
	var file_bfr []byte = bytes.Repeat([]byte("a"), 100)
	var payload = make([]byte, 0, 0)

	e := prepare_test_file(folder, file, file_bfr)
	if e != nil {
		t.Errorf("failed to create file for test")
	}
	go http.ListenAndServe(fmt.Sprintf(":%s", port), http.FileServer(http.Dir(folder)))

	rand.Seed(time.Now().UTC().UnixNano())
	workers_wg := sync.WaitGroup{}
	url := fmt.Sprintf("http://%s/%s", host, file)
	wl := worker_load{req_count: uint64(rand.Int31n(500)),
		duration: duration{time.Duration(time.Second * 1)},
		port:     port}
	header := make(map[string]string)

	var contentType string = "text/html"
	wl.Prepare_request(contentType, header, method,
		url, string(payload))
	server := fmt.Sprintf("%s:%s", host, port)
	worker := NewWorker(server, false, url)
	workers_wg.Add(1)
	worker.run_worker(&wl, &workers_wg,0,0)
	workers_wg.Wait()
	if worker.error_count > 0 {
		t.Errorf("test ended with errors")
	}
	if worker.results.count != wl.req_count {
		t.Errorf("count mismatch req=%d, actual=%d", wl.req_count, worker.results.count)
	} else {
		t.Logf("workload executed %d requests", worker.results.count)
		t.Logf("%v", worker.results.codes)
	}
	if worker.results.codes[200] != wl.req_count {
		t.Errorf("something went wrong, status codes %v", worker.results.codes)
	}
}

func Test_PUT_Worker(t *testing.T) {
	host := "127.0.0.1"
	port := "8080"
	method := "PUT"
	file := "http_blaster1.html"
	folder := "/tmp"
	var payload []byte = bytes.Repeat([]byte("a"), 100)

	go http.ListenAndServe(fmt.Sprintf(":%s", port), http.FileServer(http.Dir(folder)))

	e := prepare_test_file(folder, file, payload)
	if e != nil {
		t.Errorf("failed to create file for test")
	}

	rand.Seed(time.Now().UTC().UnixNano())
	workers_wg := sync.WaitGroup{}
	url := fmt.Sprintf("http://%s/%s", host, file)
	wl := worker_load{req_count: uint64(rand.Int31n(500)),
		duration: duration{time.Duration(time.Second * 10)},
		port:     port}
	header := make(map[string]string)
	//header["range"] = "-1"

	var contentType string = "text/html"
	wl.Prepare_request(contentType, header, method,
		url, string(payload))
	server := fmt.Sprintf("%s:%s", host, port)
	worker := NewWorker(server, false, url)
	workers_wg.Add(1)
	worker.run_worker(&wl, &workers_wg,0,0)
	workers_wg.Wait()
	if worker.results.count != wl.req_count {
		t.Errorf("count mismatch req=%d, actual=%d", wl.req_count, worker.results.count)
	} else {
		t.Logf("workload executed %d requests", worker.results.count)
		t.Logf("%v", worker.results.codes)
	}
	if worker.results.codes[200] != wl.req_count {
		t.Errorf("something went wrong, status codes %v", worker.results.codes)
	}
}
