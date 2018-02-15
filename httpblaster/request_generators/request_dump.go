package request_generators

type RequestDump struct {
	Host 	string
	Method 	string
	URI    	string
	Body	string
	Headers map[string]string
}
