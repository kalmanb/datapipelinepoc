## Run

```bash
docker-compose up
cd api
go build && ./api &
cd ../reader
go build && ./reader &
cd ../updater
go build && ./updater

## Updater

Appends rows to db.log

## Reader

Reads db.log for updates and sends rows to api

## API

Listens for updates from reader and writes them to the queue
Uses:

* protobufs
* kafka

## Subscriber

Listens to the queue and writes update to the DB

## Rest

Rest API for polling the DB

## TODO

* Create backpressure between reader and api - wait for response - then get next batch
```
