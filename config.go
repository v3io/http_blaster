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
	Duration        duration
	Block_size      int32
	Server          string
	Port            string
	TSLMode         bool
	StatusCodesDist map[string]float64
}

type workload struct {
	Name      string
	Bucket    string
	File_path string
	Type      CommandType
	Duration  duration
	Count     uint64
	Workers   int
	Id        int32
	Header    map[string]string
	Payload   string
}

func LoadConfig(file_path string) (tomlConfig, error) {
	var config tomlConfig
	if _, err := toml.DecodeFile(file_path, &config); err != nil {
		return config, err
	}
	return config, nil
}
