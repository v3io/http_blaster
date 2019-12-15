package tests

import (
	"github.com/v3io/http_blaster/httpblaster/utils"
	"testing"
)

func Test_utils_print(t *testing.T) {
	utils.PrintTime()
}

func Test_utils_generate_year_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("year")
	print(str)
}
func Test_utils_generate_month_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("month")
	print(str)
}
func Test_utils_generate_day_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("day")
	print(str)
}

func Test_utils_generate_hour_partitioned_request(t *testing.T) {
	str := utils.GeneratePartitionedRequest("hour")
	print(str)
}
