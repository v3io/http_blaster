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
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
)

var (
	start_time   time.Time
	end_time     time.Time
	wl_id        int32 = 0
	conf_file    string
	results_file string
	showVersion  bool
	dataBfr      []byte
	cpu_profile  = false
	mem_profile  = false
	config       httpblaster.TomlConfig
	executors    []*httpblaster.Executor
	ex_group     sync.WaitGroup
	enable_log   bool
	log_file     *os.File
)

const AppVersion = "2.0.0"

func init() {
	const (
		default_conf         = "example.toml"
		usage_conf           = "conf file path"
		usage_version        = "show version"
		usage_results_file   = "results file path"
		default_results_file = "example.results"
		default_log_file     = true
	)
	flag.StringVar(&conf_file, "conf", default_conf, usage_conf)
	flag.StringVar(&conf_file, "c", default_conf, usage_conf+" (shorthand)")
	flag.StringVar(&results_file, "o", default_results_file, usage_results_file+" (shorthand)")
	flag.BoolVar(&showVersion, "v", false, usage_version)
	flag.BoolVar(&cpu_profile, "p", false, "write cpu profile to file")
	flag.BoolVar(&mem_profile, "m", false, "write mem profile to file")
	flag.BoolVar(&enable_log, "d", default_log_file, "enable stdout to log")
}

func get_workload_id() int32 {
	defer atomic.AddInt32(&wl_id, 1)
	return wl_id
}

func start_cpu_profile() {
	if cpu_profile {
		log.Println("CPU Profile enabled")
		f, err := os.Create("cpu_profile")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
}

func stop_cpu_profile() {
	if cpu_profile {
		pprof.StopCPUProfile()
	}
}

func write_mem_profile() {
	if mem_profile {
		log.Println("MEM Profile enabled")
		f, err := os.Create("mem_profile")
		defer f.Close()
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
	}
}

func parse_cmd_line_args() {
	flag.Parse()
	if showVersion {
		log.Printf("Running Version %s", AppVersion)
		os.Exit(0)
	}
}

func load_test_Config() {
	var err error
	config, err = httpblaster.LoadConfig(conf_file)
	if err != nil {
		log.Println(err)
		log.Fatalln("Failed to parse config file")
	}
	log.Printf("Running test on %s:%s, tls mode=%v, block size=%d, test timeout %v",
		config.Global.Server, config.Global.Port, config.Global.TLSMode,
		config.Global.Block_size, config.Global.Duration)
	dataBfr = make([]byte, config.Global.Block_size, config.Global.Block_size)
	for i, _ := range dataBfr {
		dataBfr[i] = byte(rand.Int())
	}

}

func generate_executors() {
	for Name, workload := range config.Workloads {
		log.Println("Adding executor for ", Name)
		workload.Id = get_workload_id()
		e := &httpblaster.Executor{Workload: workload, Host: config.Global.Server,
			Port: config.Global.Port, Tls_mode: config.Global.TLSMode,
			StatusCodesAcceptance: config.Global.StatusCodesAcceptance, Data_bfr: dataBfr}
		executors = append(executors, e)
	}
}

func start_executors() {
	ex_group.Add(len(executors))
	start_time = time.Now()
	for _, e := range executors {
		e.Start(&ex_group)
	}
}

func wait_for_completion() {
	log.Println("Wait for executors to finish")
	ex_group.Wait()
	end_time = time.Now()
}

func report_executor_result(file string) {
	fname := fmt.Sprintf("%s.executors", file)
	f, err := os.Create(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for _, executor := range executors {
		r, e := executor.Report()
		if e != nil {
			f.WriteString(e.Error())
		} else {
			f.WriteString("======================================\n")
			f.WriteString(fmt.Sprintf("Duration = %v\n", r.Duration))
			f.WriteString(fmt.Sprintf("Iops = %v\n", r.Iops))
			f.WriteString(fmt.Sprintf("Statuses = %v\n", r.Statuses))

			f.WriteString(fmt.Sprintf("Avg = %v\n", r.Avg))
			f.WriteString(fmt.Sprintf("Max = %v\n", r.Max))
			f.WriteString(fmt.Sprintf("Min = %v\n", r.Min))
			f.WriteString(fmt.Sprintf("Latency = %v\n", r.Latency))

			f.WriteString(fmt.Sprintf("Total = %v\n", r.Total))
			f.WriteString(fmt.Sprintf("Errors = %v\n", r.Errors))
		}
	}
}

func report() int {
	var overall_requests uint64 = 0
	var overall_get_requests uint64 = 0
	var overall_put_requests uint64 = 0
	var overall_get_lat_max time.Duration = 0
	var overall_get_lat_min time.Duration = 0
	var overall_put_lat_max time.Duration = 0
	var overall_put_lat_min time.Duration = 0
	var overall_iops uint64 = 0
	var overall_get_iops uint64 = 0
	var overall_put_iops uint64 = 0
	var overall_get_avg_lat time.Duration = 0
	var overall_put_avg_lat time.Duration = 0
	errors := make([]error, 0)
	duration := end_time.Sub(start_time)
	for _, executor := range executors {
		results, err := executor.Report()
		if err != nil {
			errors = append(errors, err)
		}
		overall_requests += results.Total
		if executor.Workload.Type == "GET" {
			overall_get_requests += results.Total
			overall_get_iops += results.Iops
			overall_get_avg_lat += time.Duration(float64(results.Avg) * float64(results.Total))
			if overall_get_lat_max < results.Max {
				overall_get_lat_max = results.Max
			}
			if overall_get_lat_min == 0 {
				overall_get_lat_min = results.Min
			}
			if overall_get_lat_min > results.Min {
				overall_get_lat_min = results.Min
			}
		} else {
			overall_put_requests += results.Total
			overall_put_iops += results.Iops
			overall_put_avg_lat += time.Duration(float64(results.Avg) * float64(results.Total))
			if overall_put_lat_max < results.Max {
				overall_put_lat_max = results.Max
			}
			if overall_put_lat_min == 0 {
				overall_put_lat_min = results.Min
			}
			if overall_put_lat_min > results.Min {
				overall_put_lat_min = results.Min
			}
		}

		overall_iops += results.Iops
	}

	if overall_get_requests != 0 {
		overall_get_avg_lat = time.Duration(float64(overall_get_avg_lat) / float64(overall_get_requests))
	}
	if overall_put_requests != 0 {
		overall_put_avg_lat = time.Duration(float64(overall_put_avg_lat) / float64(overall_put_requests))
	}

	log.Println("Duration: ", duration)
	log.Println("Overall Results: ")
	log.Println("Overall Requests: ", overall_requests)
	log.Println("Overall GET Requests: ", overall_get_requests)
	log.Println("Overall GET Min Latency: ", overall_get_lat_min)
	log.Println("Overall GET Max Latency: ", overall_get_lat_max)
	log.Println("Overall GET Avg Latency: ", overall_get_avg_lat)
	log.Println("Overall PUT Requests: ", overall_put_requests)
	log.Println("Overall PUT Min Latency: ", overall_put_lat_min)
	log.Println("Overall PUT Max Latency: ", overall_put_lat_max)
	log.Println("Overall PUT Avg Latency: ", overall_put_avg_lat)
	log.Println("Overall IOPS: ", overall_iops)
	log.Println("Overall GET IOPS: ", overall_get_iops)
	log.Println("Overall PUT IOPS: ", overall_put_iops)

	report_executor_result(results_file)
	f, err := os.Create(results_file)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	f.WriteString(fmt.Sprintf("[global]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overall_requests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overall_iops))
	f.WriteString(fmt.Sprintf("\n[get]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overall_get_requests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overall_get_iops))
	f.WriteString(fmt.Sprintf("overall_lat_min=%vusec\n", overall_get_lat_min.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_max=%vusec\n", overall_get_lat_max.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_avg=%vusec\n", overall_get_avg_lat.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("\n[put]\n"))
	f.WriteString(fmt.Sprintf("overall_requests=%v\n", overall_put_requests))
	f.WriteString(fmt.Sprintf("overall_iops=%v\n", overall_put_iops))
	f.WriteString(fmt.Sprintf("overall_lat_min=%vusec\n", overall_put_lat_min.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_max=%vusec\n", overall_put_lat_max.Nanoseconds()/1e3))
	f.WriteString(fmt.Sprintf("overall_lat_avg=%vusec\n", overall_put_avg_lat.Nanoseconds()/1e3))

	if len(errors) > 0 {
		for _, e := range errors {
			log.Println(e)
		}
		return 2
	}
	return 0
}

func configure_log_to_file() {
	if enable_log {
		file_name := fmt.Sprintf("%s.log", results_file)
		var err error = nil
		log_file, err = os.Create(file_name)
		if err != nil {
			log.Fatalln("failed to open log file")
		} else {
			log_writers := io.MultiWriter(os.Stdout, log_file)
			log.SetOutput(log_writers)
		}
	}
}

func close_log_file() {
	if log_file != nil {
		log_file.Close()
	}
}

func exit(err_code int) {
	if err_code != 0 {
		err := errors.New("Test failed with error")
		panic(err)
	}
	log.Println("Test completed successfully")
}

func handle_exit() {
	if err := recover(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func main() {
	parse_cmd_line_args()
	configure_log_to_file()
	log.Println("Starting http_blaster")

	defer handle_exit()
	defer close_log_file()
	defer stop_cpu_profile()
	defer write_mem_profile()

	start_cpu_profile()
	load_test_Config()
	generate_executors()
	start_executors()
	wait_for_completion()
	err_code := report()
	exit(err_code)
}
