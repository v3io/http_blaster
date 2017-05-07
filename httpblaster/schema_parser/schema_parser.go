package schema_parser

import (
	"encoding/csv"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type SchemaValue struct {
	Name string
	Type igz_data.IgzType
}

type SchemaParser struct {
	Schema_file string
	csv_map     map[int]SchemaValue
}

func StringToKind(str string) igz_data.IgzType {
	switch strings.TrimSpace(str) {
	case "StringType":
		return igz_data.T_STRING
	case "LongType":
		return igz_data.T_DOUBLE
	case "NoneType":
		return igz_data.T_NULL
	case "IntType":
		return igz_data.T_NUMBER

	}
	panic(fmt.Sprintf("unknown value type %s", str))
	return igz_data.T_NULL

}

func (self *SchemaParser) LoadSchema(file_path string) error {
	f, e := os.Open(file_path)
	self.csv_map = make(map[int]SchemaValue)
	if e != nil {
		return e
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = ','
	for i := 0; ; i++ {
		schema_value, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		log.Println(schema_value)
		self.csv_map[i] = SchemaValue{Name: schema_value[0], Type: StringToKind(schema_value[1])}
	}
	log.Println(self.csv_map)
	return nil
}

func (self *SchemaParser) JsonFromCSVRecord(vals []string) string {
	emd_item := igz_data.NewEmdItem()
	for i, v := range vals {
		emd_item.InsertItemAttr(self.csv_map[i].Name, self.csv_map[i].Type, ConvertValue(self.csv_map[i].Type, v))
	}
	return string(emd_item.ToJsonString())
}

func ConvertValue(t igz_data.IgzType, v string) interface{} {
	switch t {
	case igz_data.T_STRING:
		return v
	case igz_data.T_NUMBER:
		r, e := strconv.Atoi(v)
		if e != nil {
			panic(e)
		}
		return r
	case igz_data.T_DOUBLE:
		r, e := strconv.ParseFloat(v, 64)
		if e != nil {
			panic(e)
		}
		return r
	default:
		panic(fmt.Sprintf("missing type conversion %v", t))
	}
}
