package utils

import (
	"fmt"
	"time"
)

func GeneratePartitionedRequest(partition_by string, t time.Time) string {
	//t := time.Now()
	//t := time.Now().UTC().AddDate(0, 0, 0)
	if partition_by == "year" {
		return fmt.Sprintf("/year=%d/",
			t.Year())
	}
	if partition_by == "month" {
		return fmt.Sprintf("/year=%d/month=%02d/",
			t.Year(), t.Month())
	}
	if partition_by == "day" {
		return fmt.Sprintf("/year=%d/month=%02d/day=%02d/",
			t.Year(), t.Month(), t.Day())
	}
	if partition_by == "hour" {
		return fmt.Sprintf("/year=%d/month=%02d/day=%02d/hour=%02d/",
			t.Year(), t.Month(), t.Day(),
			t.Hour())
	}
	return ""
}
