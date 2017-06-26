package httpblaster

import (
	"encoding/csv"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"testing"
)

func Test_Schema_Parser(t *testing.T) {
	p:=  igz_data.EmdSchemaParser{}
	e := p.LoadSchema("../example/schema_example.json")
	if e != nil {
		t.Error(e)
	}

	f, err := os.Open("../examples/payloads/order-book-sample.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = '|'

	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		log.Println(record)

		j := p.EmdFromCSVRecord()
		log.Println(j)

	}
}
