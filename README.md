## Updater

Appends rows to db.log

## Reader

Reads db.log for updates and sends rows to api

## API

Listens for updates from reader and writes them to the queue

## Subscriber

Listens to the queue and writes update to the DB
