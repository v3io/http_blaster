package igz_data

import "encoding/base64"

type StreamRecord struct {
	ClientInfo   string
	Data         string
	PartitionKey string
	ShardId      int
}

func (self *StreamRecord) GetData() string {
	data, err := base64.StdEncoding.DecodeString(self.Data)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (self *StreamRecord) SetData(data string) {
	self.Data = base64.StdEncoding.EncodeToString([]byte(data))
}

type StreamRecords struct {
	NextLocation int
	LagInBytes   int
	LagInRecord  int
	RecordsNum   int
	Records      []StreamRecord
}

func NewStreamRecord(clientInfo string, data string, partition_key string, shard_id int) StreamRecord {
	r := StreamRecord{
		ClientInfo: 	clientInfo,
		PartitionKey:	partition_key,
		ShardId:    	shard_id,
	}
	r.SetData(data)
	return r
}
