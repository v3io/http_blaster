package igz_data

import (
	"encoding/json"
//	"errors"
	"fmt"
//	log "github.com/sirupsen/logrus"
	"strconv"
	"github.com/v3io/v3io-tsdb/pkg/utils"

	//"go/parser"

)

type IgzTSDBItem struct {
	Lset utils.Labels
	Time string
	Value float64
}

type IgzTSDBItems struct {
	Items []IgzTSDBItem
}


func (self *IgzTSDBItem) GenerateStructs(vals []string,parser *EmdSchemaParser) ([]IgzTSDBItem,error){
	//names := parser.tsdb_name
	self.InsertTSDBName(vals,parser)
	self.InsertTime(vals ,parser)
	self.InsertValue(vals[parser.tsdb_value_index])
	return nil,nil
}


func (self *IgzTSDBItem) GenerateStruct(vals []string,parser *EmdSchemaParser) error{
	self.InsertTSDBName(vals,parser)
	self.InsertTime(vals ,parser)
	self.InsertValue(vals[parser.tsdb_value_index])
	return nil
}


func (self *IgzTSDBItem) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzTSDBItem) InsertTSDBName(vals []string,parser *EmdSchemaParser) error {
	//parser.tsdb_name_index= -1
	//for _, v := range parser.values_map {
	//	if v.Name == parser.tsdb_name {
	//		parser.tsdb_name_index = v.Index
	//	}
	//}
	parser.tsdb_name_index=GetIndexByValue(parser.values_map,parser.tsdb_name)
	input :=""
	if parser.tsdb_name_index > -1 {
		input = vals[parser.tsdb_name_index]
	}	else {
	input = parser.tsdb_name
	}
	self.InsertName(input)
	for key, val := range parser.tsdb_attributes_map {
		lable := utils.Label{Name: key, Value: vals[val]}
		self.Lset=  append(self.Lset,lable)
		}
	return nil
}

func (self *IgzTSDBItem) InsertTime(vals []string,parser *EmdSchemaParser) error {
	for _, v := range parser.values_map {
		if v.Name == parser.tsdb_time {
			parser.tsdb_time_index = v.Index
		}
	}
	input := vals[parser.tsdb_time_index]
	//add validation on time
	self.InsertTimeString(input)
	return nil
}

func (self *IgzTSDBItem) InsertTimeString(strVal string){
	self.Time=strVal
}

func (self *IgzTSDBItem) InsertName(strVal string){
	self.Lset = utils.Labels{{Name: "__name__",Value:strVal}}
}

func (self *IgzTSDBItem) InsertValue(strVal string){
	f, err := strconv.ParseFloat(strVal, 64)
	if err!=nil {
		panic(fmt.Sprintf("conversion error to float %v %v", strVal))
	}
	self.Value=f
}

func GetIndexByValue(vals map[int]SchemaValue,val string) (int){
	for _, v := range vals {
		if v.Name == val {
			return v.Index
		}
	}
	return -1
}

/*
func (self *IgzTSDBItemQuery) InsertKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.Key)
		log.Error(err)
		return errors.New(err)
	}
	self.Key[key] = make(map[string]interface{})
	self.Key[key][string(value_type)] = value
	return nil
}
*/