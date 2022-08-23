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
package igz_data

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"io/ioutil"
	"regexp"	
	"strings"
)

type Schema struct {
	Settings SchemaSettings
	Columns  []SchemaValue
}

type SchemaSettings struct {
	Format       string
	Separator    config.Sep
	KeyFields    string
	KeyFormat    string
	UpdateFields string
	TSDBName string
	TSDBTime string
	TSDBValue  string
	TSDBLables string

}

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
	JsonSchema           Schema
	update_fields        string
	update_fields_indexs []int
	updateMode           string
	updateExpression     string
	tsdb_name 			 string
	tsdb_name_index		 int
	tsdb_time 			 string
	tsdb_time_index 	 int
	tsdb_value 			 string
	tsdb_value_index	 int
	tsdb_attributes 	 string
	tsdb_attributes_map  map[string]int

}

func (self *EmdSchemaParser) LoadSchema(file_path, update_mode, update_expression string) error {

	self.values_map = make(map[int]SchemaValue)
	self.tsdb_attributes_map = make(map[string]int)
	plan, _ := ioutil.ReadFile(file_path)
	err := json.Unmarshal(plan, &self.JsonSchema)
	if err != nil {
		panic(err)
	}
	columns := self.JsonSchema.Columns
	settings := self.JsonSchema.Settings

	self.schema_key_format = settings.KeyFormat
	self.schema_key_fields = settings.KeyFields
	self.updateMode = update_mode
	self.updateExpression = update_expression
	self.tsdb_time =settings.TSDBTime
	self.tsdb_name =settings.TSDBName
	self.tsdb_value =settings.TSDBValue
	self.tsdb_attributes = settings.TSDBLables


	for _, v := range columns {
		self.values_map[v.Index] = v
	}
	self.GetKeyIndexes()
	self.MapTSDBLablesIndexes()
	self.GetTSDBNameIndex()
	self.GetTSDBValueIndex()
	if len(self.updateExpression) > 0 {
		self.GetUpdateExpressionIndexes()
	}
	return nil
}

func (self *EmdSchemaParser) GetUpdateExpressionIndexes() {
	r := regexp.MustCompile(`\$[a-zA-Z_]+`)
	matches := r.FindAllString(self.updateExpression, -1)

	for _, key := range matches {
		self.updateExpression = strings.Replace(self.updateExpression, key, "%v", 1)
		k := strings.Trim(key, "$")
		for i, v := range self.values_map {
			if v.Name == k {
				self.update_fields_indexs = append(self.update_fields_indexs, i)
			}
		}
	}
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

func (self *EmdSchemaParser) GetTSDBNameIndex() {
		for _, v := range self.values_map {
			if v.Name == self.tsdb_name {
				self.tsdb_name_index = v.Index
			}
		}
	}

func (self *EmdSchemaParser) GetTSDBValueIndex() {
	for _, v := range self.values_map {
		if v.Name == self.tsdb_value {
			self.tsdb_value_index = v.Index
		}
	}
}

func (self *EmdSchemaParser) MapTSDBLablesIndexes() {
	attributes := strings.Split(self.tsdb_attributes, ",")
	for _, att := range attributes {
		for _, v := range self.values_map {
			if v.Name == att {
				self.tsdb_attributes_map[att] = v.Index			}
		}
	}
}

func (self *EmdSchemaParser) GetFieldsIndexes(fields, delimiter string) []int {
	keys := strings.Split(fields, delimiter)
	indexArray := make([]int, 1)

	for _, key := range keys {
		for i, v := range self.values_map {
			if v.Name == key {
				indexArray = append(indexArray, i)
			}
		}
	}
	return indexArray
}

func (self *EmdSchemaParser) KeyFromCSVRecord(vals []string) string {
	//when no keys, generate random
	if len(self.schema_key_indexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(self.schema_key_indexs) == 1 {
		//fix bug of returning always key in position 0
		return vals[self.schema_key_indexs[0]]
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range self.schema_key_indexs {
		keys = append(keys, vals[i])
	}
	key := fmt.Sprintf(self.schema_key_format, keys...)
	return key
}

func (self *EmdSchemaParser) nameIndexFromCSVRecord(vals []string) string {
	//when no keys, generate random
	if len(self.schema_key_indexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(self.schema_key_indexs) == 1 {
		//fix bug of returning always key in position 0
		return vals[self.schema_key_indexs[0]]
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
		if val , ok :=self.values_map[i] ; ok  {
			err, igz_type, value := ConvertValue(val.Type, v)
			if err != nil {
				panic(fmt.Sprintf("conversion error %d %v %v", i, v, self.values_map[i]))
			}
			emd_item.InsertItemAttr(self.values_map[i].Name, igz_type, value)
		}
	}
	return string(emd_item.ToJsonString())
}

func (self *EmdSchemaParser) TSDBFromCSVRecord(vals []string) string {
	tsdb_item := IgzTSDBItem{}
	tsdb_item.GenerateStruct(vals,self)
	return string(tsdb_item.ToJsonString())
}


func (self *EmdSchemaParser) TSDBItemsFromCSVRecord(vals []string) []string{
	tsdb_item := IgzTSDBItem{}
	tsdb_item.GenerateStruct(vals,self)
	//return string(tsdb_item.ToJsonString())
	return nil
}





func (self *EmdSchemaParser) EmdUpdateFromCSVRecord(vals []string) string {
	emd_update := NewEmdItemUpdate()
	//emd_update.InsertKey("key", T_STRING, self.KeyFromCSVRecord(vals))
	emd_update.UpdateMode = self.updateMode
	var fields []interface{}
	for _, i := range self.update_fields_indexs {
		fields = append(fields, vals[i])
	}
	if len(fields) > 0 {
		emd_update.UpdateExpression = fmt.Sprintf(self.updateExpression, fields...)
	} else {
		emd_update.UpdateExpression = self.updateExpression
	}
	return string(emd_update.ToJsonString())
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
		//fmt.Println("indexes ",i, len(self.values_map))
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
		//r, e := strconv.ParseFloat(v, 64)
		//if e != nil {
		//	panic(e)
		//}
		//val := fmt.Sprintf("%.1f", r)
		//return e, T_NUMBER, val
		return nil, T_NUMBER, v
	default:
		return errors.New(fmt.Sprintf("missing type conversion %v", t)), T_STRING, ""
	}
}
