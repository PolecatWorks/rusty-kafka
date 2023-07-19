# Objective

Try out kafka for sending/receiving avro messages

Download kafka community to try out from here: https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html#get-the-software


# Start Kafka





Start zookeeper

    /opt/homebrew/opt/zookeeper/bin/zkServer start

start kafka on cli

    /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.propertie


# Build a perf tester to chase messages

Create a service that reads a kafka message, identifies a timestamp. Then modifies the timestamp to now and then writes the old timestamp in the previous timestamp location. Then send the message onto the kafka topic.
At this point it can immediately pick it up again and rinse repeat. When writing write a specific id for the service to the message as well to indicate who wrote it.
Allow it to be configured to read and write from configurable topics so we can chase between services OR we can max out a given service or give a stack of messages to a service to see the effect of a number in flight.


# Schema Registry commands

    curl -X GET http://localhost:8081/subjects
    curl -X GET http://localhost:8081/subjects/test3-value/versions
    curl -X DELETE http://localhost:8081/subjects/output0-value
    curl -X GET http://localhost:8081/subjects\?deleted=true
    curl -X DELETE http://localhost:8081/subjects/test3-value\?permanent=true
