
KAFKA_BOOTSTRAP := localhost:9092

export CONFLUENT_HOME := $(HOME)/Development/kafka/confluent-7.3.1
SCHEMA_REGISTRY_START:=$(CONFLUENT_HOME)/bin/schema-registry-start
ZOOKEEPER_SERVER_START:=$(CONFLUENT_HOME)/bin/zookeeper-server-start
KAFKA_SERVER_START:=$(CONFLUENT_HOME)/bin/kafka-server-start
KAFKA_TOPICS:=$(CONFLUENT_HOME)/bin/kafka-topics




start-zookeeper:
	$(ZOOKEEPER_SERVER_START) $(CONFLUENT_HOME)/etc/kafka/zookeeper.properties

start-kafka:
	$(KAFKA_SERVER_START) $(CONFLUENT_HOME)/etc/kafka/server.properties

start-schema:
	$(SCHEMA_REGISTRY_START) $(CONFLUENT_HOME)/etc/schema-registry/schema-registry.properties



topics-list:
	$(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) --list

topics-create:
	$(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) --create --topic "test.topic"

topics-delete:
	$(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) --delete --topic "test.topic"





benchmark:
	@cargo criterion
	@open target/criterion/reports/index.html
