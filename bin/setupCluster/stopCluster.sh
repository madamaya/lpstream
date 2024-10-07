#!/bin/bash

source $(dirname $0)/../config.sh

# Stop Flink Server
if [[ ${flinkIP} == "localhost" ]]; then
  echo "*** Stop flink cluster ***"
  ${FLINK_HOME}/bin/stop-cluster.sh
fi

# Delete topics
for i in LR NYC NYC2 YSB YSB2 Nexmark Nexmark2 Syn1 Syn2 Syn3 Syn10
do
  for j in i o l
  do
    echo "*** Delete topic (${i}-${j}) ***"
    ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${i}-${j} --bootstrap-server ${bootstrapServers}
  done
done
sleep 10

if [[ ${bootstrapServers} == *localhost* ]]; then
  # Stop kafka server
  echo "*** Stop kafka cluster ***"
  ${KAFKA_HOME}/bin/kafka-server-stop.sh
  sleep 30

  # Stop zookeeper server
  echo "*** Stop zookeeper ***"
  ${KAFKA_HOME}/bin/zookeeper-server-stop.sh
fi