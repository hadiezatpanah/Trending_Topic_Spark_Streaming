#!/usr/bin/env bash

# blocks until kafka is reachable
echo 'waiting for kafka ...'
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo 'Creating kafka topics'
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic json_topic --replication-factor 1 --partitions 1

echo 'Successfully created the following topics:'
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo 'Starting to load json file into kafka topic'
{ read -r; while read -r line; do echo "$line";sleep 1; done } < /opt/bitnami/kafka/cluster_init/resources/meetup.json | /opt/bitnami/kafka/bin/kafka-console-producer.sh --broker-list  kafka:9092 --topic json_topic
