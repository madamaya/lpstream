#!/bin/bash

source $(dirname $0)/../config.sh

# Start Flink Server
if [[ ${flinkIP} == "localhost" ]]; then
  echo "*** Start flink cluster ***"
  ${FLINK_HOME}/bin/start-cluster.sh
fi

# Start kafka cluster
if [[ ${bootstrapServers} == *localhost* ]]; then
  # Start zookeeper server
  echo "*** Start zookeeper ***"
  ${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
  sleep 10

  # Start kafka server
  echo "*** Start kafka cluster ***"
  ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
  sleep 10
fi

# Create topics
for i in LR NYC NYC2 YSB YSB2 Nexmark Nexmark2 Syn1 Syn2 Syn3
do
  for j in i o l
  do
    ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${i}-${j} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  done
done
