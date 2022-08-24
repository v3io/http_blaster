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
package tests

import (
	"github.com/v3io/http_blaster/httpblaster/igz_data"
	"testing"
)

var item2 = igz_data.IgzEmdItem{}

//var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
//var metric = randSeq(10)
//var lables = map[string]string{"dc": "7",	"hostname":   "myhosts"}
//var timestamp = NowAsUnixMilli()
//var float_val = randFloat(0,rand.Float64())
//var lable_key = "lable"
//var lable_val = "lable_value"

//func init() {
//	item2.InsertMetric(metric)
//	item2.InsertLable(lable_key,lable_val)
//	item2.InsertSample(timestamp, float_val)
//
//	fmt.Println(item2.Samples)
//	/* load test data */
//}
//
//func Test_igz_tsdb_item_v2_init(t *testing.T) {
//	assert.Equal(t, metric, item2.Metric, "they should be equal")
//}
//
//func Test_igz_tsdb_item_v2_lables(t *testing.T) {
//	assert.Equal(t, lable_val, item2.Labels[lable_key], "they should be equal")
//}
//
//func Test_igz_tsdb_item_v2_sample(t *testing.T) {
//	assert.Equal(t, timestamp, item2.Samples[0].T, "they should be equal")
//	assert.Equal(t, float_val, item2.Samples[0].V["n"], "they should be equal")
//}

func Test__igz_tsdb_item_v2_convert(t *testing.T) {
	print(item2.ToJsonString())
}

//func NowAsUnixMilli() string {
//	ts := time.Now().UnixNano() / 1e6
//	ts_str := strconv.FormatInt(ts, 10)
//	return ts_str
//}
//
//func randSeq(n int) string {
//	b := make([]rune, n)
//	for i := range b {
//		b[i] = letters[rand.Intn(len(letters))]
//	}
//	return string(b)
//}
//
//func randFloat(min, max float64) float64 {
//	res := min + rand.Float64() * (max - min)
//	return res
//}
