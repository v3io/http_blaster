# http_blaster - go http load tool

## Getting started

### Get the code into your workspace
    cd $GOPATH
    git clone https://github.com/v3io/http_blaster.git

### Build
    cd $GOPATH/http_blaster
    go get
    go build

### Generate workload file
    title = "Workload example" - name for the test workload 
    [global]
    	Duration        "120s" - test total timeout
  		Block_size      100 - payload of 100 bytes if no payload file given
	    Server        127.0.0.1 - server address
	    Port          8080 - server port
	    TSLMode       false - use secure connection
	    [global.StatusCodesDist]
        	200 = 100   -accept upto 100% from total responses to be with status code 200
        	500 = 0.1   -accept upto 0.1% from total responses to be with status code 500

    [workloads]
    [workload.1]
      	Name      "test" - name for the workload (will be written in the log)
	    Bucket    "1" - bucket will be used to build the access url
	    File_path "test.html" - file to access
	    Type      "PUT" - can be PUT or GET
	    Duration  "30s" - time to send requests
	    Count     0 - number of requests to send, if 0 is given then time is ending condition.
	    Workers   1 - number of workers to send the request in parallel
      	Payload   "payloadfile.dat" - file path that contains the payload of the request
	    [workload.1.Header]
          	range = "-1" - header key value for the request
     [workload.2]
     ...
     
	    

### Run the tool
    ./http_blaster -c [your workload file] -o [result file name]


