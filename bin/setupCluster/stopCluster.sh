#!/bin/bash

source $(dirname $0)/../config.sh

# Stop Flink Server
#echo "*** Stop flink cluster ***"
#${FLINK_HOME}/bin/stop-cluster.sh

# Delete topics
for i in LR LR2 NYC YSB Nexmark Nexmark2 Test
do
  for j in i o l
  do
    echo "*** Delete topic (${i}-${j}) ***"
    ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${i}-${j} --bootstrap-server ${bootstrapServers}
  done
done
sleep 10

# Stop kafka server
#echo "*** Stop kafka cluster ***"
#${KAFKA_HOME}/bin/kafka-server-stop.sh
#sleep 30

# Stop zookeeper server
#echo "*** Stop zookeeper ***"
#${KAFKA_HOME}/bin/zookeeper-server-stop.sh