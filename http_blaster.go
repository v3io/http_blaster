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
	"flag"
	"fmt"
	"github.com/Gurpartap/logrus-stack"
	log "github.com/sirupsen/logrus"
	"github.com/v3io/http_blaster/httpblaster"
	"github.com/v3io/http_blaster/httpblaster/config"
	"github.com/v3io/http_blaster/httpblaster/tui"
	"io"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
	//"github.com/v3io/http_blaster/httpblaster/histogram"
	"sort"
)

var (
	start_time     time.Time
	end_time       time.Time
	wl_id          int32 = -1
	conf_file      string
	results_file   string
	showVersion    bool
	dataBfr        []byte
	cpu_profile    = false
	mem_profile    = false
	cfg            config.TomlConfig
	executors      []*httpblaster.Executor
	ex_group       sync.WaitGroup
	enable_log     bool
	log_file       *os.File
	worker_qd      int  = 10000
	verbose        bool = false
	enable_ui      bool
	ch_put_latency chan time.Duration
	ch_get_latency chan time.Duration
	//LatencyCollectorGet histogram.LatencyHist// tui.LatencyCollector
	//LatencyCollectorPut histogram.LatencyHist//tui.LatencyCollector
	//StatusesCollector   tui.StatusesCollector
	term_ui       *tui.Term_ui
	dump_failures bool   = true
	dump_location string = "."
)

const AppVersion = "3.0.3"

func init() {
	const (
		default_conf          = "example.toml"
		usage_conf            = "conf file path"
		usage_version         = "show version"
		default_showversion   = false
		usage_results_file    = "results file path"
		default_results_file  = "example.results"
		usage_log_file        = "enable stdout to log"
		default_log_file      = true
		default_worker_qd     = 10000
		usage_worker_qd       = "queue depth for worker requests"
		usage_verbose         = "print debug logs"
		default_verbose       = false
		usage_memprofile      = "write mem profile to file"
		default_memprofile    = false
		usage_cpuprofile      = "write cpu profile to file"
		default_cpuprofile    = false
		usage_enable_ui       = "enable terminal ui"
		default_enable_ui     = false
		usage_dump_failures   = "enable 4xx status requests dump to file"
		defaule_dump_failures = false
		usage_dump_location   = "location of dump requests"
		default_dump_location = "."
	)
	flag.StringVar(&conf_file, "conf", default_conf, usage_conf)
	flag.StringVar(&conf_file, "c", default_conf, usage_conf+" (shorthand)")
	flag.StringVar(&results_file, "o", default_results_file, usage_results_file+" (shorthand)")
	flag.BoolVar(&showVersion, "version", default_showversion, usage_version)
	flag.BoolVar(&cpu_profile, "p", default_cpuprofile, usage_cpuprofile)
	flag.BoolVar(&mem_profile, "m", default_memprofile, usage_memprofile)
	flag.BoolVar(&enable_log, "d", default_log_file, usage_log_file)
	flag.BoolVar(&verbose, "v", default_verbose, usage_verbose)
	flag.IntVar(&worker_qd, "q", default_worker_qd, usage_worker_qd)
	flag.BoolVar(&enable_ui, "u", default_enable_ui, usage_enable_ui)
	flag.BoolVar(&dump_failures, "f", defaule_dump_failures, usage_dump_failures)
	flag.StringVar(&dump_location, "l", default_dump_location, usage_dump_location)
}

func get_workload_id() int {
	return int(atomic.AddInt32(&wl_id, 1))
}

func start_cpu_profile() {
	if cpu_profile {
		log.Println("CPU Profile enabled")
		f, err := os.Create("cpu_profile")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
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
	cfg, err = config.LoadConfig(conf_file)
	if err != nil {
		log.Println(err)
		log.Fatalln("Failed to parse config file")
	}
	log.Printf("Running test on %s:%s, tls mode=%v, block size=%d, test timeout %v",
		cfg.Global.Servers, cfg.Global.Port, cfg.Global.TLSMode,
		cfg.Global.Block_size, cfg.Global.Duration)
	dataBfr = make([]byte, cfg.Global.Block_size, cfg.Global.Block_size)
	for i, _ := range dataBfr {
		dataBfr[i] = byte(rand.Int())
	}

}

func generate_executors(term_ui *tui.Term_ui) {
	//ch_put_latency = LatencyCollectorPut.New()
	//ch_get_latency = LatencyCollectorGet.New()
	//ch_statuses := StatusesCollector.New(160, 1)

	for Name, workload := range cfg.Workloads {
		log.Println("Adding executor for ", Name)
		workload.Id = get_workload_id()

		e := &httpblaster.Executor{
			Globals:        cfg.Global,
			Workload:       workload,
			Host:           cfg.Global.Server,
			Hosts:          cfg.Global.Servers,
			TLS_mode:       cfg.Global.TLSMode,
			Data_bfr:       dataBfr,
			TermUi:         term_ui,
			Ch_get_latency: ch_get_latency,
			Ch_put_latency: ch_put_latency,
			//Ch_statuses:    ch_statuses,
			DumpFailures: dump_failures,
			DumpLocation: dump_location}
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
	//close(ch_get_latency)
	///close(ch_put_latency)
}

func wait_for_ui_completion(ch_done chan struct{}) {
	if enable_ui {
		select {
		case <-ch_done:
			break
		case <-time.After(time.Second * 10):
			close(ch_done)
			break
		}
	}
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

	report_executor_result(results_file)

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

func configure_log() {

	log.SetFormatter(&log.TextFormatter{ForceColors: true,
		FullTimestamp:   true,
		TimestampFormat: "2006/01/02-15:04:05"})
	if verbose {
		log.SetLevel(log.DebugLevel)
		log.AddHook(logrus_stack.StandardHook())
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if enable_log {
		file_name := fmt.Sprintf("%s.log", results_file)
		var err error = nil
		log_file, err = os.Create(file_name)
		if err != nil {
			log.Fatalln("failed to open log file")
		} else {
			var log_writers io.Writer
			if enable_ui {
				log_writers = io.MultiWriter(log_file, term_ui)
			} else {
				log_writers = io.MultiWriter(os.Stdout, log_file)
			}
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
		log.Errorln("Test failed with error")
		os.Exit(err_code)
	}
	log.Println("Test completed successfully")
}

func handle_exit() {
	if err := recover(); err != nil {
		log.Println(err)
		log.Exit(1)
	}
}

func enable_tui() chan struct{} {
	if enable_ui {
		term_ui = &tui.Term_ui{}
		ch_done := term_ui.Init_term_ui(&cfg)
		go func() {
			defer term_ui.Terminate_ui()
			tick := time.Tick(time.Millisecond * 500)
			for {
				select {
				case <-ch_done:
					return
				case <-tick:
					//term_ui.Update_put_latency_chart(LatencyCollectorPut.Get())
					//term_ui.Update_get_latency_chart(LatencyCollectorGet.Get())
					//term_ui.Update_status_codes(StatusesCollector.Get())
					term_ui.Refresh_log()
					term_ui.Render()
				}
			}
		}()
		return ch_done
	}
	return nil
}

func dump_latencies_histograms() {
	latency_get := make(map[int64]int)
	latency_put := make(map[int64]int)
	total_get := 0
	total_put := 0

	for _, e := range executors {
		hist := e.LatencyHist()
		if e.GetType() == "GET" {
			for k, v := range hist {
				latency_get[k] += v
				total_get += v
			}
		} else {
			for k, v := range hist {
				latency_put[k] += v
				total_put += v
			}
		}
	}
	dump_latency_histogram(latency_get, total_get, "GET")
	dump_latency_histogram(latency_put, total_put, "PUT")

}

func remap_latency_histogram(hist map[int64]int) map[int64]int {
	res := make(map[int64]int)
	for k, v := range hist {
		if k > 10000 { //1 sec
			res[10000] += v
		} else if k > 5000 { //500 mili
			res[5000] += v
		} else if k > 1000 { // 100mili
			res[1000] += v
		} else if k > 100 { //10 mili
			res[100] += v
		} else if k > 50 { //5 mili
			res[50] += v
		} else if k > 20 { //2 mili
			res[20] += v
		} else if k > 10 { //1 mili
			res[10] += v
		} else { //below 1 mili
			res[k] += v
		}
	}
	return res
}

func dump_latency_histogram(histogram map[int64]int, total int, req_type string) ([]string, []float64) {
	var keys []int
	var prefix string
	title := "type \t usec \t\t percentage\n"
	if req_type == "GET" {
		prefix = "GetHist"
	} else {
		prefix = "PutHist"
	}
	strout := fmt.Sprintf("%s Latency Histograms:\n", prefix)
	hist := remap_latency_histogram(histogram)
	for k := range hist {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	log.Debugln("latency hist wait released")
	res_strings := []string{}
	res_values := []float64{}

	for _, k := range keys {
		v := hist[int64(k)]
		res_strings = append(res_strings, fmt.Sprintf("%5d", k*100))
		value := float64(v*100) / float64(total)
		res_values = append(res_values, value)
	}

	if len(res_strings) > 0 {
		strout += title
		for i, v := range res_strings {
			strout += fmt.Sprintf("%s: %s \t\t %3.4f%%\n", prefix, v, res_values[i])
		}
	}
	log.Println(strout)
	return res_strings, res_values
}


func main() {
	parse_cmd_line_args()
	load_test_Config()
	ch_done := enable_tui()
	configure_log()
	log.Println("Starting http_blaster")

	//defer handle_exit()
	//defer close_log_file()
	defer stop_cpu_profile()
	defer write_mem_profile()

	start_cpu_profile()
	generate_executors(term_ui)
	start_executors()
	wait_for_completion()
	log.Println("Executors done!")
	dump_latencies_histograms()
	//dump_status_code_histogram()
	err_code := report()
	log.Println("Done with error code ", err_code)
	wait_for_ui_completion(ch_done)
	exit(err_code)
}
