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
	"github.com/BurntSushi/toml"
	"time"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type tomlConfig struct {
	Title     string
	Global    global
	Workloads map[string]workload
}

type global struct {
	Duration              duration
	Block_size            int32
	Server                string
	Port                  string
	TLSMode               bool
	StatusCodesAcceptance map[string]float64
}

type workload struct {
	Name       string
	Bucket     string
	File_path  string
	Type       CommandType
	Duration   duration
	Count      uint64
	Workers    int
	Id         int32
	Header     map[string]string
	Payload    string
	FileIndex  int
	FilesCount int
}

func LoadConfig(file_path string) (tomlConfig, error) {
	var config tomlConfig
	if _, err := toml.DecodeFile(file_path, &config); err != nil {
		return config, err
	}
	return config, nil
}
