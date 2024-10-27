#!/bin/zsh

# path
# Please fill in the path of l3steram
L3_HOME="~~~/lpstream"
FLINK_HOME="${L3_HOME}/flink"
KAFKA_HOME="${L3_HOME}/kafka"
BIN_DIR="${L3_HOME}/bin"
JAR_PATH="${L3_HOME}/target/lpstream-1.0-SNAPSHOT.jar"
CHECKPOINT_DIR="${L3_HOME}/data/checkpoints"

# cluster
cpmIP="localhost"
cpmPort=10010
redisIP="localhost"
redisPort=6379
flinkIP="localhost"
flinkPort=8081
flinkTMIP="localhost"
zookeeperIP="localhost"
bootstrapServers="localhost:9092"
ingestNode="localhost"

# execution
parallelism=4
numOfSamples=5
