package httpblaster

import (
	"testing"
	"github.com/v3io/http_blaster/httpblaster/igz_data"
)

func TestStreamRecord(T *testing.T)  {
	r:= igz_data.NewStreamRecord("testclinet", "testdata", "test_partition_key", 2, false)
	sr := igz_data.NewStreamRecords(r)
	T.Log(sr.ToJsonString())
}


func TestStreamRecordNoSharded(T *testing.T)  {
	r:= igz_data.NewStreamRecord("testclinet", "testdata", "test_partition_key", 2, true)
	sr := igz_data.NewStreamRecords(r)
	T.Log(sr.ToJsonString())
}