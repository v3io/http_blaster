package igz_data

import (
	"encoding/csv"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"os"
	"strconv"
	"strings"
)

type SchemaValue struct {
	Name     string
	Type     IgzType
	Nullable bool
	Key      bool
}

type EmdSchemaParser struct {
	Schema_file       string
	csv_map           map[int]SchemaValue
	schema_key_indexs []int
	schema_key_format string
	schema_key_fields string
	schema_key_seperator string
}

func StringToKind(str string) IgzType {
	switch strings.TrimSpace(str) {
	case "StringType":
		return T_STRING
	case "LongType":
		return T_DOUBLE
	case "NoneType":
		return T_NULL
	case "IntType":
		return T_NUMBER

	}
	panic(fmt.Sprintf("unknown value type %s", str))
	return T_NULL

}

func (self *EmdSchemaParser) IsNullable(v string) bool {
	v_list := strings.Split(v, "=")
	if len(v_list) > 1 {
		if strings.TrimSpace(v_list[0]) == "nullable" && strings.TrimSpace(v_list[1]) == "true" {
			return true
		}
	}
	return false

}

func (self *EmdSchemaParser) LoadSchema(file_path string, key_fields string, key_format string) error {
	self.schema_key_format = key_format
	self.schema_key_fields = key_fields
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
		self.csv_map[i] = SchemaValue{Name: schema_value[0], Type: StringToKind(schema_value[1]),
			Nullable: self.IsNullable(schema_value[2])}
	}
	self.GetKeyIndexes()
	return nil
}
func (self *EmdSchemaParser) GetKeyIndexes() {
	keys := strings.Split(self.schema_key_fields, ",")
	for _,key := range keys {
		for i, v := range self.csv_map {
			if v.Name == key {
				self.schema_key_indexs = append(self.schema_key_indexs, i)
			}
		}
	}
}

func (self *EmdSchemaParser) KeyFromCSVRecord(vals []string) string {
	if len(self.schema_key_indexs)== 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	var keys [] interface{}
	for _,i := range self.schema_key_indexs{
		keys = append(keys, vals[i])
	}
	key := fmt.Sprintf(self.schema_key_format, keys...)
	return key
}

func (self *EmdSchemaParser) JsonFromCSVRecord(vals []string) string {
	emd_item := NewEmdItem()
	k:= self.KeyFromCSVRecord(vals)
	//panic(k)
	emd_item.InsertKey("key", T_STRING, self.KeyFromCSVRecord(vals))
	for i, v := range vals {
		err, igz_type, value := ConvertValue(self.csv_map[i].Type, v)
		if err != nil {
			panic(fmt.Sprintf("conversion error ", i, v, self.csv_map[i].Name, self.csv_map[i].Type))
		}
		emd_item.InsertItemAttr(self.csv_map[i].Name, igz_type, value)
	}
	//panic(emd_item.ToJsonString())
	return string(emd_item.ToJsonString())
}

func ConvertValue(t IgzType, v string) (error, IgzType, interface{}) {
	switch t {
	case T_STRING:
		return nil, T_STRING, v
	case T_NUMBER:
		return nil, T_NUMBER, v
	case T_DOUBLE:
		r, e := strconv.ParseFloat(v, 64)
		if e != nil {
			panic(e)
		}
		val := fmt.Sprintf("%.1f", r)
		return e, T_NUMBER, val
	default:
		panic(fmt.Sprintf("missing type conversion %v", t))
	}
}
