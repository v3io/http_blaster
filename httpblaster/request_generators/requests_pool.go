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
package request_generators

import (
	"github.com/valyala/fasthttp"
	"sync"
)

type Request struct {
	Cookie  interface{}
	Id      int64
	Request *fasthttp.Request
}

type Response struct {
	Cookie   interface{}
	Id       int64
	Response *fasthttp.Response
}

var (
	requestPool  sync.Pool
	responsePool sync.Pool
)

func AcquireRequest() *Request {
	v := requestPool.Get()
	if v == nil {
		return &Request{Request: fasthttp.AcquireRequest()}
	}
	return v.(*Request)
}
func ReleaseRequest(req *Request) {
	req.Request.Reset()
	requestPool.Put(req)
}

func AcquireResponse() *Response {
	v := responsePool.Get()
	if v == nil {
		return &Response{Response: fasthttp.AcquireResponse()}
	}
	return v.(*Response)
}

func ReleaseResponse(resp *Response) {
	resp.Response.Reset()
	responsePool.Put(resp)
}
