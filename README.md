## Process

dblogger -> db.log -> reader -> api -> kafka -> aggregator -> kafka -> service(subscribe) -> cassandra -> service(rest)

## DBLogger

Appends rows to db.log

## Reader

Reads db.log for updates and sends rows to api

## API

Listens for updates from reader and writes them to the queue
Uses:

* protobufs
* kafka

## Aggregator

Enriches the data stream with aggregations

## Service

Subscribes to the queue and writes update to the DB
Has a rest api to query the data

## Run

```bash
docker-compose up
cd api
go build && ./api &
cd ../reader
go build && ./reader &
cd ../dblogger
go build && ./dblogger
```

## TODO

* service subscription
* service http
* Report end to end
* Create backpressure between reader and api - wait for response - then get next batch
* Add aggregation
* Handle high load
* Handle reconnects
