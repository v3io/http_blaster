package igz_data

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/nuclio/nuclio/pkg/errors"
	"fmt"
)

type IgzType string

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

type IgzEmdItem struct {
	//TableName           string
	//ConditionExpression string
	Key  map[string]map[string]interface{}
	Item map[string]map[string]interface{}
}

func (self *IgzEmdItem) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzEmdItem) InsertKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.Key)
		log.Error(err)
		return errors.New(err)
	}
	self.Key[key] = make(map[string]interface{})
	self.Key[key][string(value_type)] = value
	return nil
}

func (self *IgzEmdItem) InsertItemAttr(attr_name string, value_type IgzType, value interface{}) error {
	if _, ok := self.Item[attr_name]; ok {
		err := fmt.Sprintf("Key %s Override existing item %v", attr_name, self.Item)
		log.Error(err)
		return errors.New(err)
	}
	self.Item[attr_name] = make(map[string]interface{})
	self.Item[attr_name][string(value_type)] = value
	return nil
}

func NewEmdItem() *IgzEmdItem {
	i := &IgzEmdItem{}
	i.Key = make(map[string]map[string]interface{})
	i.Item = make(map[string]map[string]interface{})
	return i
}


type IgzEmdItemUpdate struct {
	//TableName           string
	UpdateMode 	    string
	UpdateExpression    string
	Key  map[string]map[string]interface{}
}

func (self *IgzEmdItemUpdate) InsertKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.Key)
		log.Error(err)
		return errors.New(err)
	}
	self.Key[key] = make(map[string]interface{})
	self.Key[key][string(value_type)] = value
	return nil
}

func (self *IgzEmdItemUpdate) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func NewEmdItemUpdate() *IgzEmdItemUpdate {
	i := &IgzEmdItemUpdate{}
	i.Key = make(map[string]map[string]interface{})
	return i
}

type IgzEmdItemQuery struct {
	TableName       string
	AttributesToGet string
	Key             map[string]map[string]interface{}
}

func (self *IgzEmdItemQuery) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzEmdItemQuery) InsertKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.Key)
		log.Error(err)
		return errors.New(err)
	}
	self.Key[key] = make(map[string]interface{})
	self.Key[key][string(value_type)] = value
	return nil
}

func NewEmdItemQuery() *IgzEmdItemQuery {
	q := &IgzEmdItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
}

type IgzEmdItemsQuery struct {
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

func (self *IgzEmdItemsQuery) ToJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}

func (self *IgzEmdItemsQuery) InsertStartingKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.StartingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.StartingKey)
		log.Error(err)
		return errors.New(err)
	}
	self.StartingKey[key] = make(map[string]interface{})
	self.StartingKey[key][string(value_type)] = value
	return nil
}

func (self *IgzEmdItemsQuery) InsertEndingKey(key string, value_type IgzType, value interface{}) error {
	if _, ok := self.EndingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, self.EndingKey)
		log.Error(err)
		return errors.New(err)
	}
	self.EndingKey[key] = make(map[string]interface{})
	self.EndingKey[key][string(value_type)] = value
	return nil
}

func NewEmdItemsQuery() *IgzEmdItemQuery {
	q := &IgzEmdItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
}
