#!/bin/zsh

source $(dirname $0)/../config.sh

function readOutputFromEarliest () {
  logDir=$1
  logFile=$2
  outputTopicName=$3

  if [ ! -d ${logDir} ]; then
    mkdir ${logDir}
  fi

  echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReaderFromEarliest ${outputTopicName} ${logDir}/${logFile} ${parallelism})"
  java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3DataReaderFromEarliest ${outputTopicName} ${logDir}/${logFile} ${parallelism}
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