package test

import (
	"encoding/csv"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"testing"
	//"go/parser"
	"strings"
)





func Test_Schema_Parser(t *testing.T) {
	//pwd, _ := os.Getwd()
	p := igz_data.EmdSchemaParser{}
	e := p.LoadSchema("../examples/schemas/schema_example.json","","")
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

		j := p.EmdFromCSVRecord(record)
		log.Println(j)

	}
}


func Test_tsdb_Schema_Parser(t *testing.T) {
	//pwd, _ := os.Getwd()
	p := igz_data.EmdSchemaParser{}
	e := p.LoadSchema("../examples/schemas/tsdb_schema_example.json","","")
	if e != nil {
		t.Error(e)
	}

	f, err := os.Open("../examples/payloads/order-book-sample.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = p.JsonSchema.Settings.Separator.Rune
	var line_count = 0
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		if strings.HasPrefix(record[0], "#") {
			log.Println("Skipping scv header ", strings.Join(record[:], ","))
		} else {
		j := p.TSDBFromCSVRecord(record)
			log.Println(j)
			line_count++
			if line_count%1024 == 0 {
				//log.Printf("line: %d from file %s was submitted", line_count)
			}
		}
	}



}
//
//func Test_tsdb_to_json(t *testing.T) {
//	item := igz_data.IgzTSDBItem{}
//	item.Labels = utils.Labels{{Name: "__name__", Value: "name"}}
//	item.Time = "1529659800000"
//	item.Value = 1
//	item2 := igz_data.IgzTSDBItem{}
//	item2.Lset = utils.Labels{{Name: "__name__", Value: "name2"}}
//	item2.Time = "1529659900000"
//	item2.Value = 2
//
//	items := []igz_data.IgzTSDBItem{item, item2}
//	body, _ := json.Marshal(items)
//	fmt.Println(string(body))
//}









