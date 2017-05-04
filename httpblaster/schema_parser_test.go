package httpblaster

import (
	"testing"
	"github.com/v3io/http_blaster/httpblaster/schema_parser"
	"os"
	"encoding/csv"
	"io"
	"log"
)

func Test_Schema_Parser(t *testing.T) {
	p := schema_parser.SchemaParser{}
	e:=p.LoadSchema("../schema1.txt")
	if e!= nil{
		t.Error(e)
	}


	f, err := os.Open("/Users/sasilevi/Downloads/order-book-sample.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = '|'

	for{
		record, err := r.Read()
		if err != nil{
			if err == io.EOF{
				break
			}
			panic(err)
		}
		log.Println(record)


		j := p.JsonFromCSVRecord(record)
		log.Println(j)

	}
}
