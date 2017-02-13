#!/bin/bash
echo "title =\"Workload put\"

[global]
block_size = 4096
Duration = \"160s\"
server=\"192.168.202.21\"
port=\"8081\"
TLSMode=false
 [global.StatusCodesAcceptance]
  200 = 100.0
  204 = 100.0
  205 = 100.0

[workloads]
" > blaster_multi_put.cfg
for i in {1..100}; do echo "[workloads.$i]
    name=\"PUT-$i\"
    bucket=\"1\"
    file_path=\"$i/test_file\"
    Duration = \"7200s\"
    TYPE=\"PUT\"
    workers=1
    count=3000000
    FileIndex=0
    FilesCount=3000000" >> blaster_multi_put.cfg; done


echo "title =\"Workload get\"

[global]
block_size = 4096
Duration = \"160s\"
server=\"192.168.202.21\"
port=\"8081\"
TLSMode=false
 [global.StatusCodesAcceptance]
  200 = 100.0
  204 = 100.0
  205 = 100.0

[workloads]
" > blaster_multi_get.cfg
for i in {1..100}; do echo "[workloads.$i]
    name=\"PUT-$i\"
    bucket=\"1\"
    file_path=\"$i/test_file\"
    Duration = \"300s\"
    TYPE=\"GET\"
    workers=10
    count=300000
    FileIndex=0
    FilesCount=3000000" >> blaster_multi_get.cfg; done
