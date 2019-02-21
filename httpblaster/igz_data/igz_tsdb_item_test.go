package igz_data

import (
	"testing"
	"math/rand"
	"strconv"

	"github.com/stretchr/testify/assert"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var item = IgzTSDBItem{}

func init() {
	/* load test data */
}

func Test_get_index_by_value(t *testing.T) {
}

func Test_igz_tsdb_item_add_value(t *testing.T) {
	value := rand.Float64()
	strValue := strconv.FormatFloat(value, 'f', -1, 64)
	item.InsertValue(strValue)
	assert.Equal(t, value, item.Value, "they should be equal")
}

func Test_igz_tsdb_item_add_name(t *testing.T) {
	name := randSeq(10)
	item.InsertName(name)
	outputStr :=item.Lset.Get("__name__")
	assert.Equal(t, name, outputStr, "they should be equal")
}

func Test_igz_tsdb_item_add_time(t *testing.T) {
	timeStr := randomTimestamp()
	item.InsertTimeString(timeStr)
	assert.Equal(t, timeStr, item.Time, "they should be equal")
}

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomTimestamp() string {
	randomNow := strconv.FormatInt(time.Now().UnixNano() / int64(time.Millisecond),10)
	return randomNow
}












