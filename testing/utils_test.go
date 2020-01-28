package testing

import (
	"github.com/v3io/http_blaster/httpblaster/utils"
	"testing"
	"time"
)

func Test_utils_generate_year_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("year", time.Now())
	print(str)
}
func Test_utils_generate_month_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("month", time.Now())
	print(str)
}
func Test_utils_generate_day_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("day", time.Now())
	print(str)
}

func Test_utils_generate_hour_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("hour", time.Now())
	print(str)
}
