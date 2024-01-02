#!/bin/bash

source $(dirname $0)/../config.sh

cd ${L3_HOME}

topicName=$2
inputFileName=$3
sleepTime=10
if [ $1 -eq 0 ]; then
  # recreate target input topic
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${topicName} --bootstrap-server ${bootstrapServers}
  sleep ${sleepTime}
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${topicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  # ALL DATA
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName} ${topicName}
elif [ $1 -eq 1 ]; then
  # recreate target input topic
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${topicName} --bootstrap-server ${bootstrapServers}
  sleep ${sleepTime}
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${topicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  # 1
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName}.1 ${topicName}
elif [ $1 -eq 2 ]; then
  # 2
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName}.2 ${topicName}
elif [ $1 -eq 3 ]; then
  echo "Delete topic (${topicName})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${topicName} --bootstrap-server ${bootstrapServers}
  echo "Sleep (${sleepTime} [s])"
  sleep ${sleepTime}
  echo "Create topic (${topicName})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${topicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
elif [ $1 -eq 4 ]; then
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${topicName} --bootstrap-server ${bootstrapServers}
  sleep ${sleepTime}
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${topicName} --bootstrap-server ${bootstrapServers} --partitions 1
else
  echo "Illegal args"
fi