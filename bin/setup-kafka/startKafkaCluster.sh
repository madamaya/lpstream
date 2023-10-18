#!/bin/bash

source $(dirname $0)/../config.sh
cd ${KAFKA_HOME}

# Start zookeeper server
#./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties

# Start kafka server
#./bin/kafka-server-start.sh -daemon ./config/server.properties

# Create topics
for i in LR NYC YSB Nexmark
do
  for j in i o l
  do
    ./bin/kafka-topics.sh --create --topic ${i}-${j} --bootstrap-server localhost:9092 --partitions 4
  done
done

cd ${L3_HOME}

# LR
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/LinearRoad/h1_1.csv LR-i

# NYC
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/NYC/yellow.csv NYC-i

# YSB
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/YSB/streaming-benchmarks/data/util/joined_input_10.json YSB-i

# Nexmark
java -cp ${JAR_PATH}r com.madamaya.l3stream.utils.L3DataLoader ${L3_HOME}/data/input/Nexmark/nexmark-minmin.json Nexmark-i
