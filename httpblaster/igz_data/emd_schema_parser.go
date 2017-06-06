package igz_data

import (
	//"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/nu7hatch/gouuid"
	//"io"
	"log"
	//"os"
	"errors"
	"io/ioutil"
	"strconv"
	"strings"
)

type SchemaValue struct {
	Name     string
	Type     IgzType
	Index    int
	Source   string
	Target   string
	Nullable bool
	Default  string
}

type EmdSchemaParser struct {
	Schema_file          string
	values_map           map[int]SchemaValue
	schema_key_indexs    []int
	schema_key_format    string
	schema_key_fields    string
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

//
//func (self *EmdSchemaParser) LoadSchema(file_path string, key_fields string, key_format string) error {
//	self.schema_key_format = key_format
//	self.schema_key_fields = key_fields
//	f, e := os.Open(file_path)
//	self.csv_map = make(map[int]SchemaValue)
//	if e != nil {
//		return e
//	}
//	defer f.Close()
//	r := csv.NewReader(f)
//	r.Comma = ','
//	for i := 0; ; i++ {
//		schema_value, err := r.Read()
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//			panic(err)
//		}
//		self.csv_map[i] = SchemaValue{Name: schema_value[0], Type: StringToKind(schema_value[1]),
//			Nullable: self.IsNullable(schema_value[2])}
//	}
//	self.GetKeyIndexes()
//	return nil
//}

func (self *EmdSchemaParser) LoadSchema(file_path string, key_fields string, key_format string) error {
	self.schema_key_format = key_format
	self.schema_key_fields = key_fields
	self.values_map = make(map[int]SchemaValue)
	plan, _ := ioutil.ReadFile(file_path)
	var data map[string][]map[string]interface{}
	err := json.Unmarshal(plan, &data)
	if err != nil {
		panic(err)
	}
	columns := data["COLUMNS"]
	for i, v := range columns {
		var c_index int = i                    //default by order
		var c_name string = v["Name"].(string) //mandatory
		var c_type string = v["Type"].(string) //mandatory
		var c_json_source string = ""          //default empty
		var c_nullable bool = true             //default true
		var c_default string = ""

		if index, ok := v["Index"]; ok {
			c_index = int(index.(float64))
		}
		if json_source, ok := v["Source"]; ok {
			c_json_source = json_source.(string)
		}
		if json_default, ok := v["Default"]; ok {
			c_default = json_default.(string)
		}
		if nullable, ok := v["Nullable"]; ok {
			c_nullable = nullable.(bool)
		}
		self.values_map[c_index] =
			SchemaValue{
				Name:     c_name,
				Index:    c_index,
				Type:     StringToKind(c_type),
				Nullable: c_nullable,
				Source:   c_json_source,
				Default:  c_default}
		log.Println(fmt.Sprintf("%+v", self.values_map[c_index]))
	}
	self.GetKeyIndexes()
	return nil
}

func (self *EmdSchemaParser) GetKeyIndexes() {
	keys := strings.Split(self.schema_key_fields, ",")
	for _, key := range keys {
		for i, v := range self.values_map {
			if v.Name == key {
				self.schema_key_indexs = append(self.schema_key_indexs, i)
			}
		}
	}
}

func (self *EmdSchemaParser) KeyFromCSVRecord(vals []string) string {
	//when no keys, generate random
	if len(self.schema_key_indexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(self.schema_key_indexs) == 1 {
		return vals[0]
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range self.schema_key_indexs {
		keys = append(keys, vals[i])
	}
	key := fmt.Sprintf(self.schema_key_format, keys...)
	return key
}

func (self *EmdSchemaParser) EmdFromCSVRecord(vals []string) string {
	emd_item := NewEmdItem()
	emd_item.InsertKey("key", T_STRING, self.KeyFromCSVRecord(vals))
	for i, v := range vals {
		err, igz_type, value := ConvertValue(self.values_map[i].Type, v)
		if err != nil {
			panic(fmt.Sprintf("conversion error %d %v %v", i, v, self.values_map[i]))
		}
		emd_item.InsertItemAttr(self.values_map[i].Name, igz_type, value)
	}
	//panic(emd_item.ToJsonString())
	return string(emd_item.ToJsonString())
}

func (self *EmdSchemaParser) HandleJsonSource(source string) []string {
	var out []string
	arr := strings.Split(source, ".")
	for _, a := range arr {
		out = append(out, handle_offset(a)...)
	}
	return out
}

func handle_offset(str string) []string {
	var res []string
	vls := strings.Split(str, "]")
	if len(vls) == 1 && !strings.HasSuffix(str, "]") {
		res = append(res, vls...)
	}
	for _, k := range vls {
		if strings.HasPrefix(k, "[") {
			res = append(res, k+"]")
		} else {
			vl := strings.Split(k, "[")
			if len(vl) == 2 {
				res = append(res, vl[0])
				res = append(res, "["+vl[1]+"]")
			}
		}
	}
	return res
}

func (self *EmdSchemaParser) KeyFromJsonRecord(json_obj []byte) string {
	//when no keys, generate random
	if len(self.schema_key_indexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(self.schema_key_indexs) == 1 {
		source_arr := self.HandleJsonSource(self.values_map[self.schema_key_indexs[0]].Source)
		s, _, _, e := jsonparser.Get(json_obj, source_arr...)
		if e != nil {
			panic(fmt.Sprintf("%v, %+v", e, source_arr))
		}
		return string(s)
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range self.schema_key_indexs {
		fmt.Println("indexes ", i, len(self.values_map))
		source_arr := self.HandleJsonSource(self.values_map[i].Source)
		s, _, _, e := jsonparser.Get(json_obj, source_arr...)
		if e != nil {
			panic(e)
		} else {
			keys = append(keys, string(s))
		}
	}
	key := fmt.Sprintf(self.schema_key_format, keys...)
	return key
}

func (self *EmdSchemaParser) EmdFromJsonRecord(json_obj []byte) (string, error) {
	emd_item := NewEmdItem()
	emd_item.InsertKey("key", T_STRING, self.KeyFromJsonRecord(json_obj))
	for _, v := range self.values_map {
		source_arr := self.HandleJsonSource(v.Source)
		var str []byte
		var e error
		str, _, _, e = jsonparser.Get(json_obj, source_arr...)
		if e != nil {
			if e == jsonparser.KeyPathNotFoundError {
				if v.Nullable {
					continue
				} else if v.Default != "" {
					str = []byte(v.Default)
				} else {
					return "", errors.New(fmt.Sprintf("%v, %+v", e, v.Source))
				}
			} else {
				return "", errors.New(fmt.Sprintf("%v, %+v", e, v.Source))
			}
		}
		err, igz_type, value := ConvertValue(v.Type, string(str))
		if err != nil {
			return "", errors.New(fmt.Sprintf("%v, %+v", err, v.Source))
		}
		emd_item.InsertItemAttr(v.Name, igz_type, value)
	}
	return string(emd_item.ToJsonString()), nil
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
		return errors.New(fmt.Sprintf("missing type conversion %v", t)), T_STRING, ""
	}
}
