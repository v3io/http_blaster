package test

import (
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"testing"
)

var strArr []string

func Test_igz_tsdb_item_v2_init(t *testing.T) {
	gen := data_generator.MemoryGenerator{}
	gen.GenerateRandomData("1")
}
