
KAFKA_BOOTSTRAP := localhost:9092

start-zookeeper:
	/opt/homebrew/opt/zookeeper/bin/zkServer start-foreground

start-kafka:
	kafka-server-start /opt/homebrew/etc/kafka/server.properties

kafka-list:
	kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --list

kafka-create:
	kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --create --topic "test.topic"

kafka-delete:
	kafka-topics --bootstrap-server $(KAFKA_BOOTSTRAP) --delete --topic "test.topic"


benchmark:
	@cargo criterion
	@open target/criterion/reports/index.html
