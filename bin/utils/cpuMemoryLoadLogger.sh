#!/bin/zsh

source $(dirname $0)/../config.sh

function startCpuMemoryLogger() {
  logDir=$1
  logFile=$2

  mkdir -p ${logDir}

  echo "startCpuMemoryLogger"
  # get taskmanagers
  echo "get taskmanagers"
  echo "(TMids=(\`curl ${flinkIP}:${flinkPort}/taskmanagers | jq '.taskmanagers[] | .id'\`))"
  TMids=(`curl ${flinkIP}:${flinkPort}/taskmanagers | jq '.taskmanagers[] | .id'`)

  # start cpuMemoryLogger.py
  cd ../utils
  echo "start cpuMemoryLogger.py"
  echo "(python cpuMemoryLogger.py ${flinkIP}:${flinkPort} ${logDir}/${logFile} ${TMids})"
  python cpuMemoryLogger.py ${flinkIP}:${flinkPort} ${logDir}/${logFile} ${TMids} 2>&1 /dev/null
  cd ../templates
}

function stopCpuMemoryLogger() {
  pid=(`ps aux | grep "cpuMemoryLogger.py" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
  done
}