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
	instance *request_pool
	once sync.Once
)

type request_pool struct {
}

func RequestPool() *request_pool {
	once.Do(func() {
		instance = &request_pool{}
	})
	return instance
}

func (self *request_pool) NewRequest(request *fasthttp.Request,
	cookie interface{}) Request {
	return Request{Request: request, Cookie: cookie}
}
