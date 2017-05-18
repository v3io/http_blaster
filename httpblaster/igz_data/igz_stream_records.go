package igz_data

import (
	"encoding/base64"
	"encoding/json"
)

const (
	ClientInfo   = "ClientInfo"
	DATA         = "Data"
	PartitionKey = "PartitionKey"
	ShardId      = "ShardId"
)

type StreamRecord map[string]interface {

}

func (self StreamRecord) GetData() string {
	data, err := base64.StdEncoding.DecodeString(self[DATA].(string))
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (self StreamRecord) SetData(data string) {
	self[DATA] = base64.StdEncoding.EncodeToString([]byte(data))
}

func (self StreamRecord) SetClientInfo(clinet_info string) {
	self[ClientInfo] = clinet_info
}

func (self StreamRecord) SetPartitionKey(partition_key string) {
	self[PartitionKey] = partition_key
}

func (self StreamRecord) SetShardId(shard_id int) {
	self[ShardId] = shard_id
}

type StreamRecords struct {
	//NextLocation int
	//LagInBytes   int
	//LagInRecord  int
	//RecordsNum   int
	Records []StreamRecord
}

func (self *StreamRecords) ToJsonString() string {
	body, err := json.Marshal(self)
	if err != nil {
		panic(err)
	}
	return string(body)
}

func NewStreamRecord(clientInfo string, data string, partition_key string,
	shard_id int, shard_round_robin bool) StreamRecord {

	r := StreamRecord{}
	r = make(map[string]interface{})
	r.SetClientInfo(clientInfo)
	if shard_round_robin == false {
		r.SetShardId(shard_id)
		r.SetPartitionKey(partition_key)
	}
	r.SetData(data)
	return r
}

func NewStreamRecords(record StreamRecord) StreamRecords {
	r := StreamRecords{}
	r.Records = append(r.Records, record)
	return r
}
