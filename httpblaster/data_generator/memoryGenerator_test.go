package data_generator

import (
	"testing"
)

var strArr []string

func Test_igz_tsdb_item_v2_init(t *testing.T) {
	gen := MemoryGenerator{}
	gen.GenerateRandomData(1)
}
