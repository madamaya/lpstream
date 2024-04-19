#!/bin/zsh

source $(dirname $0)/../config.sh

function readOutputFromEarliest () {
  exit 1
  logDir=$1
  logFile=$2
  outputTopicName=$3

  echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReaderFromEarliest ${outputTopicName} ${logDir}/${logFile} ${parallelism})"
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReaderFromEarliest ${outputTopicName} ${logDir}/${logFile} ${parallelism}
}

function readOutputLatest () {
  outputTopicName=$1
  outputFileDir=$2
  key=$3
  echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderV2 ${outputTopicName} ${parallelism} ${outputFileDir} ${key})"
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderV2 ${outputTopicName} ${parallelism} ${outputFileDir} ${key}
}

function latencyCalcFromKafka () {
  outputTopicName=$1
  outputFilePath=$2
  echo "(java -Xmx110G -cp ${JAR_PATH} com.madamaya.l3stream.utils.LatencyCalcFromKafka ${outputTopicName} ${parallelism} ${outputFilePath})"
  java -Xmx110G -cp ${JAR_PATH} com.madamaya.l3stream.utils.LatencyCalcFromKafka ${outputTopicName} ${parallelism} ${outputFilePath}
}

function latencyCalcFromKafkaDisk () {
  outputTopicName=$1
  outputFileDir=$2
  key=$3
  echo "(java -Xmx110G -cp ${JAR_PATH} com.madamaya.l3stream.utils.LatencyCalcFromKafkaDisk ${outputTopicName} ${parallelism} ${outputFileDir} ${key})"
  java -Xmx110G -cp ${JAR_PATH} com.madamaya.l3stream.utils.LatencyCalcFromKafkaDisk ${outputTopicName} ${parallelism} ${outputFileDir} ${key}
}

function startKafkaLogger () {
  logDir=$1
  logFile=$2
  outputTopicName=$3

  while :
  do
    if [ ! -d ${logDir} ]; then
      mkdir ${logDir}
    fi

    echo "java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReader ${outputTopicName} ${logFile}"
    java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReader ${outputTopicName} ${logFile} &

    # sleep 10 sec
    echo "sleep 10"
    sleep 10

    # check whether no tuple has been received.
    tupleNum=`wc -l ${logFile} | awk '{print $1}'`
    if [ ${tupleNum} -eq 0 ]; then
      echo "tupleNum=${tupleNum} -> break"
      break
    fi

    echo "tupleNum=${tupleNum} -> retry"
    stopLogger
  done
}

function stopLogger() {
  # CNFM
  pid=(`ps aux | grep "L3DataReader" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
    sleep 1
  done
}