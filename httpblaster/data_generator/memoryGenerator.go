package data_generator

import (
	"github.com/shirou/gopsutil/mem"
	"fmt"
	"strconv"
	"time"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"encoding/json"
	"bytes"
	"os"
	//"reflect"
	"reflect"
)



type MemoryGenerator struct{
	Total      uint64
	Availible  uint64
	Active     uint64
}

func  (self *MemoryGenerator) GenerateRandomData() []string{
	v, _ := mem.VirtualMemory()

	payloads :=self.GenrateJsonArray(v)
	fmt.Println(payloads)
	return payloads
}

func  (self *MemoryGenerator) GenerateJsonByVal(colName string,val float64) string{
	item :=igz_data.IgzTSDBItem{}
	item.Time =  strconv.FormatInt(time.Now().Unix() ,10)
	item.Value=float64(val)
	//item.Lset=utils.Labels{{Name: "__name__",Value:colName}}
	item.Lset=utils.Labels{{Name: "__name__",Value:"memo"}}
	lable := utils.Label{Name: "host", Value:GetHostname() }
	item.Lset=  append(item.Lset,lable)
	collable := utils.Label{Name: "type", Value:colName }
	item.Lset=  append(item.Lset,collable)
	return item.ToJsonString()
}


func (self *MemoryGenerator) GenrateJsonArray(v *mem.VirtualMemoryStat) []string{
	  arr :=  []string{}
	  val := make(map[string]interface{})
	  json.Unmarshal([]byte(v.String()), &val)
	  for s,vl := range val{
	  	  f ,_ := getFloat(vl)
		  arr = append(arr, self.GenerateJsonByVal(s,f))
	  	}
	return arr
}





func (self *MemoryGenerator) ToJson() string{
	v, err := json.Marshal(&self)
	if err != nil {
		fmt.Println("There was an error:", err)
	}
	return string(v)

}

func (self *MemoryGenerator) ConvertToByteArray() {
	reqBodyBytes := new(bytes.Buffer)
	json.NewEncoder(reqBodyBytes).Encode(self)

	reqBodyBytes.Bytes() // this is the []byte

}

func GetHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	} else {
	return name }
}

var floatType = reflect.TypeOf(float64(0))

func getFloat(unk interface{}) (float64, error) {
	v := reflect.ValueOf(unk)
	v = reflect.Indirect(v)
	if !v.Type().ConvertibleTo(floatType) {
		return 0, fmt.Errorf("cannot convert %v to float64", v.Type())
	}
	fv := v.Convert(floatType)
	return fv.Float(), nil
}



func  (self *MemoryGenerator) LoopThroughStruct(){

	v, _ := mem.VirtualMemory()
	stat, _ := json.Marshal(v)
	fmt.Println(stat)
}



