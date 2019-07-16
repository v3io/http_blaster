package tests

import (
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"testing"
)

func Test_memeory_generator(t *testing.T) {
	gen := data_generator.MemoryGenerator{}
	gen.GenerateRandomData("1")
}
