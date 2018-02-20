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
