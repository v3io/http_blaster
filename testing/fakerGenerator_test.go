package testing

import (
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"testing"
	"time"
)

func Test_facker_generator(t *testing.T) {
	gen := data_generator.Fake{}
	gen.GenerateRandomData(time.Now())
}

func Test_facker_generator_to_igzemditem(t *testing.T) {
	gen := data_generator.Fake{}
	gen.GenerateRandomData(time.Now())
	str := gen.ConvertToIgzEmdItemJson()
	fmt.Println(str)
}
