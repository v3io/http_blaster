# Copyright 2016 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#!/bin/bash
GET_FILE="blaster_get.cfg"
PUT_FILE="blaster_put.cfg"
SERVER="127.0.0.1"
PORT="8081"
BUCKET="1"
FILE_NAME="test_file"

PUT_DURATION="200s"
PUT_LOAD_START=1
PUT_LOAD_END=100
PUT_LOAD_WORKERS=1
PUT_COUNT=1000
PUT_FILES_START=0
PUT_FILES_COUNT=30000

GET_DURATION="200s"
GET_COUNT=1000
GET_LOAD_START=1
GET_LOAD_END=100
GET_LOAD_WORKERS=10
GET_FILES_START=0
GET_FILES_COUNT=30000


#generate put workload file
echo "title =\"Workload put\"

[global]
block_size = 4096
Duration = \"7200s\"
server=\"$SERVER\"
port=\"$PORT\"
TLSMode=false
 [global.StatusCodesAcceptance]
  200 = 100.0
  204 = 100.0
  205 = 100.0

[workloads]
" > $PUT_FILE
for (( i=$PUT_LOAD_START; i<=$PUT_LOAD_END; i++ )) do echo "[workloads.$i]
    name=\"PUT-$i\"
    bucket=\"$BUCKET\"
    Target=\"$i/$FILE_NAME\"
    Duration = \"$PUT_DURATION\"
    TYPE=\"PUT\"
    workers=$PUT_LOAD_WORKERS
    count=$PUT_COUNT
    FileIndex=$PUT_FILES_START
    FilesCount=$PUT_FILES_COUNT" >> $PUT_FILE; done



#generate get workload file
echo "title =\"Workload get\"

[global]
block_size = 4096
Duration = \"7200s\"
server=\"$SERVER\"
port=\"$PORT\"
TLSMode=false
 [global.StatusCodesAcceptance]
  200 = 100.0
  204 = 100.0
  205 = 100.0

[workloads]
" > $GET_FILE
for (( i=$GET_LOAD_START; i<=$GET_LOAD_END; i++ )) do echo "[workloads.$i]
    name=\"GET-$i\"
    bucket=\"$BUCKET\"
    Target=\"$i/$FILE_NAME\"
    Duration = \"$GET_DURATION\"
    TYPE=\"GET\"
    workers=$GET_LOAD_WORKERS
    count=$GET_COUNT
    FileIndex=$GET_FILES_START
    FilesCount=$GET_FILES_COUNT" >> $GET_FILE; done
