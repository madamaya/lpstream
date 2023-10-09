#!/bin/bash

kafkaHome="/Users/yamada-aist/workspace/l3stream/kafka_2.12-3.4.1"

cd ${kafkaHome}

# Start zookeeper server
#./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties

# Start kafka server
#./bin/kafka-server-start.sh -daemon ./config/server.properties

# Create topics
for i in LR NYC YSB Nexmark
do
  for j in i o
  do
    ./bin/kafka-topics.sh --create --topic ${i}-${j} --bootstrap-server localhost:9092 --partitions 4
  done
done
