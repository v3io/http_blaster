package testing

import (
	//"fmt"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	//"strings"
	"testing"
)

func Test_memeory_generator(t *testing.T) {
	gen := data_generator.MemoryGenerator{}
	gen.GenerateRandomData("1")
	//fmt.Println("["+strings.Join(payloads, ", ")+"]")
}
