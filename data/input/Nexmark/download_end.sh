#!/bin/bash

cd nexmark/nexmark-flink

# stop flink cluster
cd flink
./bin/stop-cluster.sh

# stop kafka cluster
cd ../kafka
./bin/kafka-server-stop.sh
./bin/zookeeper-server-stop.sh
