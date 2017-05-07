package request_generators

import "github.com/valyala/fasthttp"

const (
	PERFORMANCE = "performance"
	CSV2STREAM  = "csv2stream"
	CSV2KV      = "csv2kv"
)

type RequestCommon struct {
}

func (self *RequestCommon) PrepareRequest(content_type string,
	header_args map[string]string,
	method string, uri string,
	body string, host string) *fasthttp.Request {
	req := fasthttp.AcquireRequest()

	header := fasthttp.RequestHeader{}
	header.SetContentType(content_type)

	header.SetMethod(method)
	header.SetRequestURI(uri)
	header.SetHost(host)

	for k, v := range header_args {
		header.Set(k, v)
	}
	req.AppendBodyString(body)
	header.CopyTo(&req.Header)
	return req
}
