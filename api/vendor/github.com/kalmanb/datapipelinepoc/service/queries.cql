
// Connect
// docker exec -ti datapoc_cassandra_1 bash

create keyspace events with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use events;

CREATE TABLE event(
    account   text,
    id        int,
    timestamp bigint,
    amount    bigint,
    field1    text,
    field2    text,
    field3    text,
    field4    text,
    field5    text,
    PRIMARY KEY (account, id)
);

select * from events;


// Reset Kafka Offsets
docker exec -ti datapoc_kafka_1 bash
bash-4.4# kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group service_sub --reset-offsets --to-earliest --all-topics --execute