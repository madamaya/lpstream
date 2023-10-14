#!/bin/zsh

# start kafka logger (args[0]: testCase name, args[2]: checkpointID)
# if specified dir does not exists, create the dir.
function startLogger () {
  testCase=$1 # example "NYC"
  logDir=$2
  logFile=$3

  while :
  do
    if [ ! -d ./log ]; then
      mkdir ./log
      mkdir ./log/${testCase}
    elif [ ! -d ./log/${testCase} ]; then
      mkdir ./log/${testCase}
    fi
    python kafkaLogger.py ${testCase} ${logFile} &

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
  pid=(`ps aux | grep "python.*kafkaLogger" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
    sleep 1
  done
}