#!/bin/zsh

source $(dirname $0)/../config.sh

function ingestData() {
  filePath=$1
  qName=$2
  topic=$3
  paral=$4
  throughput=$5
  granularity=$6

  echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoader ${filePath} ${qName} ${topic} ${paral} ${throughput} ${granularity})"
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoader ${filePath} ${qName} ${topic} ${paral} ${throughput} ${granularity}
}

function maxIngestionRate() {
    filePath=$1
    qName=$2
    topic=$3
    paral=$4

    echo "java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderTest ${filePath} ${qName} ${topic} ${paral}"
    java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderTest ${filePath} ${qName} ${topic} ${paral}
}

function stopIngestion() {
    pid=`ps aux | grep kafka | grep -v grep | awk '{print $2}'`
}