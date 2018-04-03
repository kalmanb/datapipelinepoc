## Setup

* Install protobuf - https://developers.google.com/protocol-buffers/docs/downloads

```bash
go get -u github.com/golang/protobuf/protoc-gen-go
cd .../go/src/asb/host/api
protoc -I=. --go_out=. schema.proto
```
