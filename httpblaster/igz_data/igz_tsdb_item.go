package igz_data

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"time"
	//"go/parser"
)

type IgzTSDBItem struct {
	Lset utils.Labels
	Time string
	Value float64
}

func (self *IgzTSDBItem) GenerateStruct(vals []string,parser *EmdSchemaParser) error{
	//self.InsertTSDBName(parser.tsdb_attributes_map,vals,T_STRING,vals[parser.tsdb_name_index])

	self.InsertTSDBName(vals,parser)
	self.InsertTime(vals ,parser)
	self.InsertValue(vals[parser.tsdb_value_index])
	return nil
}


func (self *IgzTSDBItem) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

//func (self *IgzTSDBItem) InsertTSDBName(attributes_map map[string]int,vals []string,value_type IgzType, value interface{}) error {
func (self *IgzTSDBItem) InsertTSDBName(vals []string,parser *EmdSchemaParser) error {
	parser.tsdb_name_index= -1
	for _, v := range parser.values_map {
		if v.Name == parser.tsdb_name {
			parser.tsdb_name_index = v.Index
		}
	}
	input :=""
	if parser.tsdb_name_index > 0 {
		input = vals[parser.tsdb_name_index]
	}	else {
	input = parser.tsdb_name
	}
	//add validation on time
	self.Time= input
	self.Lset = utils.Labels{{Name: "__name__",Value:input}}
	for key, val := range parser.tsdb_attributes_map {
		lable := utils.Label{Name: key, Value: vals[val]}
		self.Lset=  append(self.Lset,lable)
		}
	return nil
}

func (self *IgzTSDBItem) InsertKey(key string, value_type IgzType, value interface{}) error {
	strVal := value.(string)

	_,err := time.Parse( time.RFC3339,strVal)
	if err != nil	{
		//fix convert
		self.Time=strconv.FormatInt(time.Now().Unix() , 10)
	}	else{
		self.Time = strVal
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
	self.Time=  input
	return nil
}




func (self *IgzTSDBItem) InsertValue(strVal string){
	f, err := strconv.ParseFloat(strVal, 64)
	if err!=nil {
		panic(fmt.Sprintf("conversion error to float %v %v", strVal))
	}
	self.Value=f
}

func NewTSDBItem() *IgzTSDBItem {
	i := &IgzTSDBItem{}
	return i
}

type IgzTSDBItemUpdate struct {
	UpdateMode       string
	UpdateExpression string
}

func (self *IgzTSDBItemUpdate) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

type IgzTSDBItemQuery struct {
	TableName       string
	AttributesToGet string
	Key             map[string]map[string]interface{}
}

func (self *IgzTSDBItemQuery) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

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

type IgzTSDBItemsQuery struct {
	TableName        string
	AttributesToGet  string
	Limit            int
	FilterExpression string
	Segment          int
	TotalSegment     int
	Marker           string
	StartingKey      map[string]map[string]interface{}
	EndingKey        map[string]map[string]interface{}
}

func (self *IgzTSDBItemsQuery) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzTSDBItemsQuery) InsertStartingKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.StartingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.StartingKey)
		log.Error(err)
		return errors.New(err)
	}
	self.StartingKey[key] = make(map[string]interface{})
	self.StartingKey[key][string(value_type)] = value
	return nil
}

func (self *IgzTSDBItemsQuery) InsertEndingKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.EndingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.EndingKey)
		log.Error(err)
		return errors.New(err)
	}
	self.EndingKey[key] = make(map[string]interface{})
	self.EndingKey[key][string(value_type)] = value
	return nil
}


func (item IgzTSDBItem) ConvertToTSDBItem() *IgzTSDBItem{
	returnItem := IgzTSDBItem{}
	return  &returnItem
}
