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
	"fmt"
	"github.com/v3io/http_blaster/httpblaster/data_generator"
	"testing"
)

func Test_facker_generator(t *testing.T) {
	gen := data_generator.Fake{}
	gen.GenerateRandomData()
}

func Test_facker_generator_to_igzemditem(t *testing.T) {
	gen := data_generator.Fake{}
	gen.GenerateRandomData()
	str := gen.ConvertToIgzEmdItemJson()
	fmt.Println(str)
}
