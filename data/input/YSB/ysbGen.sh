#!/bin/bash

source $(dirname $0)/../../../bin/config.sh

YSB_HOME=`pwd`
numTuples=36000000

if [ $# -eq 1 ]; then
  numTuples=$1
fi

echo "=*=*=*=*= Start YSB data generation =*=*=*=*="

echo "*** Initialize redis ***"
echo "(redis-cli FLUSHDB)"
redis-cli FLUSHDB

echo "*** Download original YSB repository ***"
git clone https://github.com/yahoo/streaming-benchmarks.git

echo "*** Copy data generation code for our experiment ***"
cp core.clj streaming-benchmarks/data/src/setup

# start kafka cluster
echo "*** Start zookeeper for kafka ***"
echo "(${L3_HOME}/kafka/bin/zookeeper-server-start.sh -daemon ${L3_HOME}/kafka/config/zookeeper.properties)"
${L3_HOME}/kafka/bin/zookeeper-server-start.sh -daemon ${L3_HOME}/kafka/config/zookeeper.properties
sleep 10
echo "*** Start kafka server ***"
echo "(${L3_HOME}/kafka/bin/kafka-server-start.sh -daemon ${L3_HOME}/kafka/config/server.properties)"
${L3_HOME}/kafka/bin/kafka-server-start.sh -daemon ${L3_HOME}/kafka/config/server.properties
sleep 10
echo "*** Create kafka topic (nexmark) ***"
echo "(${L3_HOME}/kafka/bin/kafka-topics.sh --create --topic ad-events --bootstrap-server localhost:9092)"
${L3_HOME}/kafka/bin/kafka-topics.sh --create --topic ad-events --bootstrap-server localhost:9092

echo "*** Start data generation ***"
cd streaming-benchmarks/data
echo "(lein run -n --configPath ../conf/benchmarkConf.yaml)"
lein run -n --configPath ../conf/benchmarkConf.yaml
echo "(lein run -r -t 1000 --configPath ../conf/benchmarkConf.yaml > /dev/null &)"
lein run -r -t 1000 --configPath ../conf/benchmarkConf.yaml > /dev/null &

sleep 5

cd ${YSB_HOME}

echo "*** Get redis pair ***"
echo "(python getRedisPair.py)"
python getRedisPair.py

echo "*** Start logger ***"
python kafkaLogger.py ${numTuples}

echo "*** Stop data generation"
kill `ps aux | grep leiningen.core.main | grep -v grep | awk '{print $2}'`

# stop kafka cluster
echo "*** Remove topic ***"
${L3_HOME}/kafka/bin/kafka-topics.sh --delete --topic ad-events --bootstrap-server localhost:9092
sleep 10
echo "*** Stop kafka server ***"
${L3_HOME}/kafka/bin/kafka-server-stop.sh
sleep 30
echo "*** Stop zookeeper ***"
${L3_HOME}/kafka/bin/zookeeper-server-stop.sh

#echo "*** Join data ***"
#echo "(python joinData.py)"
#python joinData.py

echo "cp ../data/ysb.json ../data/ysb2.json"
cp ../data/ysb.json ../data/ysb2.json

echo "=*=*=*=*= End YSB data generation =*=*=*=*="