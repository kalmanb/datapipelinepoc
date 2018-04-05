# Kafka Partitioners for Sarama

## Default Java/Scala - Murmur2Partitioner 
This is a go implementation to match the current recommended default partitioner:

https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java

It implements a murmur2 hash that matches the partitioning provided by the official Java/Scala client.

The default hash implementation in sarama is FNV-1a which is not compatible with the standard kafka client.

## Deprecated Java/Scala - JavaHashPartitioner
TBC


## Testing

    # Get the Kafka lib
    curl -O http://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/0.10.1.0/kafka-clients-0.10.1.0.jar
    
    curl -O http://repo1.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jarhttps://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
    curl -O http://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.21/slf4j-api-1.7.21.jar
    curl -O http://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.21/slf4j-log4j12-1.7.21.jar
    curl -O http://repo1.maven.org/maven2/org/mockito/mockito-core/2.2.9/mockito-core-2.2.9.jar
    curl -O http://repo1.maven.org/maven2/net/bytebuddy/byte-buddy/1.5.0/byte-buddy-1.5.0.jar
    curl -O http://repo1.maven.org/maven2/org/objenesis/objenesis/2.4/objenesis-2.4.jar
    curl -O http://repo1.maven.org/maven2/net/bytebuddy/byte-buddy-agent/1.5.0/byte-buddy-agent-1.5.0.jar
    
    mkdir mockito-extensions
    echo "mock-maker-inline" > mockito-extensions/org.mockito.plugins.MockMaker
    
    
