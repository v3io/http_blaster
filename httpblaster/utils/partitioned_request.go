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
