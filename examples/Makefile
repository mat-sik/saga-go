.PHONY: list-topics \
	transactions-state \
	transactions-dlq-state \
	alarms-state \
	alarms-dlq-state \
	transactions-read \
	transactions-dlq-read \
	alarms-read \
	alarms-dlq-read

list-topics:
	docker exec examples-kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 \
		--list

transactions-state:
	docker exec examples-kafka-broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server localhost:19092 \
		--topic transactions \
		--time -1

transactions-dlq-state:
	docker exec examples-kafka-broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server localhost:19092 \
		--topic transactions-dlq \
		--time -1

alarms-state:
	docker exec examples-kafka-broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server localhost:19092 \
		--topic alarms \
		--time -1

alarms-dlq-state:
	docker exec examples-kafka-broker-1 /opt/kafka/bin/kafka-get-offsets.sh \
		--bootstrap-server localhost:19092 \
		--topic alarms-dlq \
		--time -1

transactions-read:
	docker exec -it examples-kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic transactions \
		--partition 0 \
		--offset 100 \
		--max-messages 10 \
		--formatter-property print.partition=true \
		--formatter-property print.offset=true \
		--formatter-property print.timestamp=true \
		--formatter-property print.key=true \
		--formatter-property print.headers=true

transactions-dlq-read:
	docker exec -it examples-kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic transactions-dlq \
		--partition 0 \
		--offset 350 \
		--max-messages 10 \
		--formatter-property print.partition=true \
		--formatter-property print.offset=true \
		--formatter-property print.timestamp=true \
		--formatter-property print.key=true \
		--formatter-property print.headers=true

alarms-read:
	docker exec -it examples-kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic alarms \
		--partition 0 \
		--offset 100 \
		--max-messages 10 \
		--formatter-property print.partition=true \
		--formatter-property print.offset=true \
		--formatter-property print.timestamp=true \
		--formatter-property print.key=true \
		--formatter-property print.headers=true

alarms-dlq-read:
	docker exec -it examples-kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic alarms-dlq \
		--partition 0 \
		--offset 100 \
		--max-messages 10 \
		--formatter-property print.partition=true \
		--formatter-property print.offset=true \
		--formatter-property print.timestamp=true \
		--formatter-property print.key=true \
		--formatter-property print.headers=true
