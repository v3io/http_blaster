package igz_data

import (
	"encoding/json"
)


type Sample struct{
	T string `json:"t"`
	V map[string]float64 `json:"v"`
}

type IgzTSDBItemV2 struct {
	Metric string	`json:"metric"`

	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

type IgzTSDBItems2 struct {
	Items []IgzTSDBItemV2
}

func (self *IgzTSDBItemV2) InsertMetric(metric string){
	self.Metric = metric
}


func (self *IgzTSDBItemV2) InsertLable(key string,value string){
	if len(self.Labels) == 0 {
		self.Labels = make( map[string]string)
	}
	self.Labels[key] = value
}

func (self *IgzTSDBItemV2) InsertLables(lables map[string]string){
	self.Labels =lables
}

func (self *IgzTSDBItemV2) InsertSample(ts string,value float64){
	s:= &Sample{}
	s.T =ts
	s.V = map[string]float64{"n":value}
	self.Samples = append(self.Samples,*s)
}


func (self *IgzTSDBItemV2) ConvertJsonString() string {
	body, _ := json.Marshal(self)
	return string(body)
}




