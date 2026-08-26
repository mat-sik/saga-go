#!/bin/sh
set -eu

BOOTSTRAP="${KAFKA_INIT_TOPICS_BOOTSTRAP_SERVER:?KAFKA_INIT_TOPICS_BOOTSTRAP_SERVER must be set}"

# name:partitions:replication
TOPICS="
transactions:5:1
transactions-dlq:5:1
alarms:5:1
alarms-dlq:5:1
"

for entry in $TOPICS; do
  name=$(echo "$entry" | cut -d: -f1)
  partitions=$(echo "$entry" | cut -d: -f2)
  replication=$(echo "$entry" | cut -d: -f3)

  echo "Creating topic: $name (partitions=$partitions, replication=$replication)"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication"
done

echo "Topic creation complete. Listing topics:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list