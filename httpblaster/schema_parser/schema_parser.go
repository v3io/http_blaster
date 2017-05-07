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
		return igz_data.T_NUMBER
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
	//for k,v:= range self.csv_map{
	//	log.Println(fmt.Sprintf("%v - %v",k,v))
	//}

	return nil
}

func (self *SchemaParser) JsonFromCSVRecord(vals []string) string {
	emd_item := igz_data.NewEmdItem()
	for i, v := range vals {
		err, value := ConvertValue(self.csv_map[i].Type, v)
		if err != nil{
			panic(fmt.Sprintf("conversion error ", i, v, self.csv_map[i].Name, self.csv_map[i].Type))
		}
		emd_item.InsertItemAttr(self.csv_map[i].Name, self.csv_map[i].Type, value)
	}
	//panic(emd_item.ToJsonString())
	return string(emd_item.ToJsonString())
}

func ConvertValue(t igz_data.IgzType, v string) (error, interface{}) {
	switch t {
	case igz_data.T_STRING:
		return nil, v
	case igz_data.T_NUMBER:
		return nil, v
		r, e := strconv.Atoi(v)
		return e, r
	case igz_data.T_DOUBLE:
		return nil, v
		r, e := strconv.ParseFloat(v, 64)
		return e, r
	default:
		panic(fmt.Sprintf("missing type conversion %v", t))
	}
}
