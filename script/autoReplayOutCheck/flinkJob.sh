#!/bin/zsh

# $1: flinkHome
function cancelFlinkJobs() {
  flinkHome=$1

  echo "get jobid"
  jobid=(`curl localhost:8081/jobs | jq '.jobs[] | select( .status == "RESTARTING" or .status == "RUNNING" ) | .id' | xargs echo`)
  for id in ${jobid[@]}
  do
    echo "${flinkHome}/bin/flink cancel ${id}"
    ${flinkHome}/bin/flink cancel ${id}
  done
}

function getRunningJobID() {
  #echo "getRunningJobID"
  jobid=(`curl localhost:8081/jobs | jq '.jobs[] | select( .status == "RUNNING" ) | .id' | xargs echo`)

  if [ ${#jobid[*]} -eq 1 ]; then
    echo ${jobid[1]}
  else
    echo "-1"
  fi
}