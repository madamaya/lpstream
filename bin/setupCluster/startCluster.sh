#!/bin/bash

source $(dirname $0)/../config.sh

# Start Flink Server
echo "*** Start flink cluster ***"
${FLINK_HOME}/bin/start-cluster.sh

# Start zookeeper server
echo "*** Start zookeeper ***"
${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
sleep 10

# Start kafka server
echo "*** Start kafka cluster ***"
${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
sleep 10

# Create topics
for i in LR NYC YSB Nexmark
do
  for j in i o l
  do
    ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${i}-${j} --bootstrap-server localhost:9092 --partitions ${parallelism}
  done
done

cd ${L3_HOME}

# LR
echo "*** Ingest LR data into Kafka ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/LinearRoad/lr.csv LR-i)"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/LinearRoad/lr.csv LR-i

# NYC
echo "*** Ingest NYC data into Kafka ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/NYC/nyc.csv NYC-i)"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/NYC/nyc.csv NYC-i

# YSB
echo "*** Ingest YSB data into Kafka ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/YSB/ysb.json YSB-i)"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/YSB/ysb.json YSB-i

# Nexmark
echo "*** Ingest Nexmark data into Kafka ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/Nexmark/nexmark.json Nexmark-i)"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/Nexmark/nexmark.json Nexmark-i
