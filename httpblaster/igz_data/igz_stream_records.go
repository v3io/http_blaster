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
