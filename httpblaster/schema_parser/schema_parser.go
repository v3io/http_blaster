package schema_parser

import (
	"encoding/csv"
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"io"
	"os"
	"strconv"
	"strings"
)

type SchemaValue struct {
	Name string
	Type igz_data.IgzType
}

type SchemaParser struct {
	Schema_file 	string
	csv_map   	map[int]SchemaValue
	schema_key_index 	int
	schema_key_name 	string

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

func (self *SchemaParser)IsKeyField(v string)  bool{
	v_list := strings.Split(v, "=")
	if len(v_list) > 1{
		if strings.TrimSpace(v_list[0]) == "key" && strings.TrimSpace(v_list[1]) == "true"{
			return true
		}
	}
	return false

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
		//log.Println(schema_value)
		self.csv_map[i] = SchemaValue{Name: schema_value[0], Type: StringToKind(schema_value[1])}
		if len(schema_value)>2{
			if self.IsKeyField(schema_value[2]){
				self.schema_key_name = schema_value[0]
				self.schema_key_index = i
			}
		}
	}
	return nil
}

func (self *SchemaParser) JsonFromCSVRecord(vals []string) string {
	emd_item := igz_data.NewEmdItem()
	for i, v := range vals {
		err, igz_type, value := ConvertValue(self.csv_map[i].Type, v)
		if err != nil{
			panic(fmt.Sprintf("conversion error ", i, v, self.csv_map[i].Name, self.csv_map[i].Type))
		}
		if self.schema_key_index == i{
			emd_item.InsertKey( self.csv_map[i].Name,igz_type, value)
		}else {
			emd_item.InsertItemAttr(self.csv_map[i].Name, igz_type, value)
		}
	}
	//panic(emd_item.ToJsonString())
	return string(emd_item.ToJsonString())
}

func ConvertValue(t igz_data.IgzType, v string) (error, igz_data.IgzType, interface{}) {
	switch t {
	case igz_data.T_STRING:
		return nil, igz_data.T_STRING, v
	case igz_data.T_NUMBER:
		return nil, igz_data.T_NUMBER, v
	case igz_data.T_DOUBLE:
		r, e := strconv.ParseFloat(v, 64)
		if e != nil{
			panic(e)
		}
		val := fmt.Sprintf("%.1f", r)
		return e, igz_data.T_NUMBER, val
	default:
		panic(fmt.Sprintf("missing type conversion %v", t))
	}
}
