package igz_data

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	//"github.com/v3io/v3io-tsdb/pkg/utils"
	"strconv"
	"github.com/v3io/v3io-tsdb/pkg/utils"
)

/*type IgzType string

const (
	T_BLOB       IgzType = "B"
	T_BOOL               = "BOOL"
	T_ATTR_LIST          = "L"
	T_ATTR_MAP           = "M"
	T_NUMBER             = "N"
	T_NUMBER_SET         = "NS"
	T_NULL               = "NULL"
	T_UNIX_TIME          = "UT"
	T_TIME_STAMP         = "TS"
	T_STRING             = "S"
	T_STRING_SET         = "SS"
	T_DOUBLE             = "D"
)
*/
type IgzTSDBItem struct {
	//TableName           string
	//ConditionExpression string
	//Key  map[string]map[string]interface{}
	Lset utils.Labels
	Time string
	//Value map[string]map[string]interface{}
	Value float64
}

func (self *IgzTSDBItem) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzTSDBItem) InsertLsetName(value_type IgzType, value interface{}) error {
	strVal := value.(string)
	self.Lset = utils.Labels{{Name: "__name__",Value:strVal}}
	return nil
}

func (self *IgzTSDBItem) InsertKey(key string, value_type IgzType, value interface{}) error {
	strVal := value.(string)
	self.Time = strVal
	return nil
}

func (self *IgzTSDBItem) InsertValue(attr_name string, value_type IgzType, value interface{}) error {
	strVal := value.(string)
	f, err := strconv.ParseFloat(strVal, 64)
	if err!=nil {
		panic(err.Error())
	}
	self.Value=f
	return nil
}

func NewTSDBItem() *IgzTSDBItem {
	i := &IgzTSDBItem{}
	//i.Key = make(map[string]map[string]interface{})
	//i.Value = make(map[string]map[string]interface{})
	return i
}

type IgzTSDBItemUpdate struct {
	//TableName           string
	UpdateMode       string
	UpdateExpression string
	//Key  map[string]map[string]interface{}
}

//
//func (self *IgzTSDBItemUpdate) InsertKey(key string, value_type IgzType, value interface{}) error {
//	if _, ok := self.Key[key]; ok {
//		err := fmt.Sprintf("Key %s Override existing key %v", key, self.Key)
//		log.Error(err)
//		return errors.New(err)
//	}
//	self.Key[key] = make(map[string]interface{})
//	self.Key[key][string(value_type)] = value
//	return nil
//}

func (self *IgzTSDBItemUpdate) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func NewTSDBItemUpdate() *IgzTSDBItemUpdate {
	i := &IgzTSDBItemUpdate{}
	//i.Key = make(map[string]map[string]interface{})
	return i
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

func NewTSDBItemQuery() *IgzTSDBItemQuery {
	q := &IgzTSDBItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
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

func NewTSDBItemsQuery() *IgzTSDBItemQuery {
	q := &IgzTSDBItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
}

func (item IgzTSDBItem) ConvertToTSDBItem() *IgzTSDBItem{
	returnItem := IgzTSDBItem{}
	/*for i, v := range item.Key {
		if val , ok :=item.Key[i] ; ok  {
			returnItem.Time = val
		}
	}*/


	return  &returnItem
}
