## Setup

* Install protobuf - https://developers.google.com/protocol-buffers/docs/downloads

```bash
go get -u github.com/golang/protobuf/protoc-gen-go
cd .../go/src/github.com/kalmanb/datapipelinepoc/api
protoc -I=. --go_out=. *.proto
```
