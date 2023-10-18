#!/bin/bash

source $(dirname $0)/../config.sh

cd ${L3_HOME}

inputTopicName=$2
inputFileName=$3

if [ $1 -eq 0 ]; then
  # recreate target input topic
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${inputTopicName} --bootstrap-server ${bootstrapServers}
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${inputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  # ALL DATA
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName} ${inputTopicName}
elif [ $1 -eq 1 ]; then
  # recreate target input topic
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${inputTopicName} --bootstrap-server ${bootstrapServers}
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${inputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  # 1
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName}.1 ${inputTopicName}
elif [ $1 -eq 2 ]; then
  # 2
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${inputFileName}.2 ${inputTopicName}
else
  echo "Illegal args"
fi