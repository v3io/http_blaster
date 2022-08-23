/*
Copyright 2016 Iguazio Systems Ltd.

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
package httpblaster

import (
	"encoding/csv"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"testing"
	//"go/parser"
	"strings"


	"github.com/v3io/v3io-tsdb/pkg/utils"
	"encoding/json"
	"fmt"
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
				log.Printf("line: %d from file %s was submitted", line_count, f)
			}
		}
	}



}

func Test_tsdb_to_json(t *testing.T) {
	item := igz_data.IgzTSDBItem{}
	item.Lset = utils.Labels{{Name: "__name__", Value: "name"}}
	item.Time = "1529659800000"
	item.Value = 1
	item2 := igz_data.IgzTSDBItem{}
	item2.Lset = utils.Labels{{Name: "__name__", Value: "name2"}}
	item2.Time = "1529659900000"
	item2.Value = 2

	items := []igz_data.IgzTSDBItem{item, item2}
	body, _ := json.Marshal(items)
	fmt.Println(string(body))
}









