#!/bin/bash

PARTITIONS=${PARTITIONS:-3}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-3}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-kafka1:9092}

echo "Waiting for Kafka broker at $BOOTSTRAP_SERVER to be available..."
while ! nc -z $(echo $BOOTSTRAP_SERVER | cut -d: -f1) $(echo $BOOTSTRAP_SERVER | cut -d: -f2); do
  sleep 2
done

kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic payments \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

kafka-topics --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic receipts \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo "Done."