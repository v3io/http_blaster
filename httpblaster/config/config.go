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

package config

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

type Sep struct {
	Rune rune
}

func (r *Sep) UnmarshalText(text []byte) error {
	if len(text) > 0 {
		data := int32(text[0])
		r.Rune = data
	}
	return nil
}

type TomlConfig struct {
	Title     string
	Global    Global
	Workloads map[string]Workload
}

type Global struct {
	Duration              duration
	Block_size            int32
	Servers               []string
	Server                string
	Port                  string
	TLSMode               bool
	StatusCodesAcceptance map[string]float64
	RetryOnStatusCodes    []int
	RetryCount 	      int
	IgnoreAttrs           []string
}

type Workload struct {
	Name        string
	Container   string
	Target      string
	Type        string
	Duration    duration
	Count       int
	Workers     int
	Id          int
	Header      map[string]string
	Payload     string
	FileIndex   int
	FilesCount  int
	Random      bool
	Generator   string
	Schema      string
	Lazy        int
	ShardCount  uint32
	ShardColumn uint32
	Separator   string
}

func LoadConfig(file_path string) (TomlConfig, error) {
	var config TomlConfig
	if _, err := toml.DecodeFile(file_path, &config); err != nil {
		return config, err
	}
	return config, nil
}
