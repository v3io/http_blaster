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
package igz_data

import (
	"encoding/json"
	"fmt"
	"strconv"
)


type Sample struct{
	T string `json:"t"`
	V map[string]float64 `json:"v"`
}

type IgzTSDBItem struct {
	Metric string	`json:"metric"`

	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

func (self *IgzTSDBItem) GenerateStruct(vals []string,parser *EmdSchemaParser) error{
	self.InsertParserMetric(vals,parser)
	self.InsertParserLables(vals,parser)
	self.InsertParserSample(vals ,parser)
	return nil
}

type IgzTSDBItems2 struct {
	Items []IgzTSDBItem
}

func (self *IgzTSDBItem) InsertMetric(metric string){
	self.Metric = metric
}


func (self *IgzTSDBItem) InsertLable(key string,value string){
	if len(self.Labels) == 0 {
		self.Labels = make( map[string]string)
	}
	self.Labels[key] = value
}

func (self *IgzTSDBItem) InsertLables(lables map[string]string){
	self.Labels =lables
}

func (self *IgzTSDBItem) InsertSample(ts string,value float64){
	s:= &Sample{}
	s.T =ts
	s.V = map[string]float64{"n":value}
	self.Samples = append(self.Samples,*s)
}


func (self *IgzTSDBItem) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}


func (self *IgzTSDBItem) InsertParserMetric(vals []string,parser *EmdSchemaParser)  {
	parser.tsdb_name_index=GetIndexByValue(parser.values_map,parser.tsdb_name)
	input :=""
	if parser.tsdb_name_index > -1 {
		input = vals[parser.tsdb_name_index]
	}	else {
		input = parser.tsdb_name
	}
	self.InsertMetric(input)
}

func (self *IgzTSDBItem) InsertParserLables(vals []string,parser *EmdSchemaParser) {
	for key, val := range parser.tsdb_attributes_map {
		self.InsertLable(key,vals[val])
	}
}

func (self *IgzTSDBItem) InsertParserSample(vals []string,parser *EmdSchemaParser) {
	for _, v := range parser.values_map {
		if v.Name == parser.tsdb_time {
			parser.tsdb_time_index = v.Index
		}
	}
	ts := vals[parser.tsdb_time_index]
	val := vals[parser.tsdb_value_index]
	f, err := strconv.ParseFloat(val, 64)
	if err!=nil {
		panic(fmt.Sprintf("conversion error to float %v ", val))
	}
	self.InsertSample(ts,f)
}

func GetIndexByValue(vals map[int]SchemaValue,val string) (int){
	for _, v := range vals {
		if v.Name == val {
			return v.Index
		}
	}
	return -1
}
