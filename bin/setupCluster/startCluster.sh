#!/bin/bash

source $(dirname $0)/../config.sh
cd ${KAFKA_HOME}

# Start zookeeper server
${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties

# Start kafka server
${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties

# Create topics
for i in LR NYC YSB Nexmark
do
  for j in i o l
  do
    ./bin/kafka-topics.sh --create --topic ${i}-${j} --bootstrap-server localhost:9092 --partitions ${parallelism}
  done
done

cd ${L3_HOME}

# LR
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/LinearRoad/h1_l3.csv LR-i

# NYC
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/NYC/yellow.csv NYC-i

# YSB
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/YSB/joined_input.json YSB-i

# Nexmark
java -cp ${JAR_PATH}r com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/Nexmark/nexmark.json Nexmark-i
