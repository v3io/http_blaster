package igz_data

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
	"io/ioutil"
	"strconv"
	"strings"
	"log"
	"regexp"
)

type Schema struct {
	Settings SchemaSettings
	Columns  []SchemaValue
}

type SchemaSettings struct {
	Format    string
	Separator config.Sep
	KeyFields string
	KeyFormat string
	UpdateFields string
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
	Schema_file       string
	values_map        map[int]SchemaValue
	schema_key_indexs []int
	schema_key_format string
	schema_key_fields string
	JsonSchema        Schema
	update_fields     string
	update_fields_indexs []int
	updateMode        string
	updateExpression  string
}

func (self *EmdSchemaParser) LoadSchema(file_path, update_mode, update_expression string) error {

	self.values_map = make(map[int]SchemaValue)
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

	for _, v := range columns {
		self.values_map[v.Index] = v
	}
	self.GetKeyIndexes()
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
		log.Println(k)
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

func (self *EmdSchemaParser) GetFieldsIndexes(fields, delimiter string ) []int{
	keys := strings.Split(fields, delimiter)
	indexArray := make([]int,1)

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
	return string(emd_item.ToJsonString())
}


func (self *EmdSchemaParser) EmdUpdateFromCSVRecord(vals []string) string {
	emd_update := NewEmdItemUpdate()
	emd_update.InsertKey("key", T_STRING, self.KeyFromCSVRecord(vals))
	emd_update.UpdateMode = self.updateMode
	var fields []interface{}
	for _, i := range self.update_fields_indexs {
		fields = append(fields, vals[i])
	}
	if len(fields) > 0 {
		emd_update.UpdateExpression = fmt.Sprintf(self.updateExpression, fields...)
	}else{
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
