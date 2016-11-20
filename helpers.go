package main

import (
	"github.com/valyala/fasthttp"
)

func clone_request(req *fasthttp.Request) *fasthttp.Request {
	new_req := new(fasthttp.Request)
	req.Header.CopyTo(&new_req.Header)
	new_req.AppendBody(req.Body())
	return new_req
}
