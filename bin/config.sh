#!/bin/zsh

# path
L3_HOME="/Users/yamada-aist/workspace/l3stream"
FLINK_HOME="${L3_HOME}/flink-1.17.1"
KAFKA_HOME="${L3_HOME}/kafka_2.12-3.4.1"
BIN_DIR="${L3_HOME}/bin"
JAR_PATH="${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar"
CHECKPOINT_DIR="${L3_HOME}/data/checkpoints"

# cluster
cpmIP="localhost"
cpmPort=10010
redisIP="localhost"
redisPort=6379
flinkIP="localhost"
flinkPort=8081
bootstrapServers="localhost:9092"

# execution
parallelism=4
numOfSamples=1