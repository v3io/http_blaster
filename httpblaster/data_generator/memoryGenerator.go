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
package data_generator

import (
	"encoding/json"
	"fmt"
	"strings"

	//"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"os"
	"reflect"
	"strconv"
	"time"
)

type MemoryGenerator struct{
	Total      uint64
	Availible  uint64
	Active     uint64
}

func  (self *MemoryGenerator) GenerateRandomData(cpuNumber string) []string{
	//stats, _ := cpu.Info()
	//fmt.Println(stats)
	v, _ := mem.VirtualMemory()
	payloads :=self.GenerateJsonArray(v ,cpuNumber)
	fmt.Println(strings.Join(payloads, ", "))
	return payloads
}

func  (self *MemoryGenerator) GenerateJsonByVal(timestamp string,colName string,val float64 , cpuNumber string) string{
	//item :=igz_data.IgzTSDBItem{}
	item :=igz_data.IgzTSDBItem{}
	item.InsertMetric("memory")
	item.InsertLable("type",colName)
	item.InsertLable("hostname",GetHostname())
	item.InsertLable("cpu",string(cpuNumber))

	item.InsertSample(timestamp,val)
	return item.ToJsonString()
}


func (self *MemoryGenerator) GenerateJsonArray(v *mem.VirtualMemoryStat,cpuNumber string) []string{
	  timestamp :=  NowAsUnixMilli()
	  arr :=  []string{}
	  val := make(map[string]interface{})
	if err := json.Unmarshal([]byte(v.String()), &val) ; err!=nil {
		panic(err)
	}
	  for s,vl := range val{
	  	  f ,_ := getFloat(vl)
		  arr = append(arr, self.GenerateJsonByVal(timestamp,s,f,cpuNumber))
	  	}
	return arr
}


func GetHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	} else {
	return name }
}

var floatType = reflect.TypeOf(float64(0))

func getFloat(unk interface{}) (float64, error) {
	v := reflect.ValueOf(unk)
	v = reflect.Indirect(v)
	if !v.Type().ConvertibleTo(floatType) {
		return 0, fmt.Errorf("cannot convert %v to float64", v.Type())
	}
	fv := v.Convert(floatType)
	return fv.Float(), nil
}

func NowAsUnixMilli() string {
	ts := time.Now().UnixNano() / 1e6
	ts_str := strconv.FormatInt(ts, 10)
	return ts_str
}
