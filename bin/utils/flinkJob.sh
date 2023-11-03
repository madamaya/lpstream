#!/bin/zsh

source $(dirname $0)/../config.sh

# $1: flinkHome
function cancelFlinkJobs() {
  echo "get jobid"
  jobid=(`curl ${flinkIP}:${flinkPort}/jobs | jq '.jobs[] | select( .status == "RESTARTING" or .status == "RUNNING" ) | .id' | xargs echo`)
  for id in ${jobid[@]}
  do
    echo "${FLINK_HOME}/bin/flink cancel ${id}"
    ${FLINK_HOME}/bin/flink cancel ${id}
  done
}

function getRunningJobID() {
  #echo "getRunningJobID"
  jobid=(`curl ${flinkIP}:${flinkPort}/jobs | jq '.jobs[] | select( .status == "RUNNING" ) | .id' | xargs echo`)

  if [ ${#jobid[*]} -eq 1 ]; then
    echo ${jobid[1]}
  else
    echo "-1"
  fi
}

function restartFlinkCluster() {
  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 15
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 15
}

function restartTMifNeeded() {
  num=`curl localhost:8081/taskmanagers | jq '.taskmanagers' | jq 'length' | awk '{print $1}'`
  if [ ${num} -eq 0 ]; then
    echo "*** no taskmanagers (runningTMnum) ***"
    echo "*** restart ***"
    echo "(restartFlinkCluster)"
    restartFlinkCluster
  fi
}