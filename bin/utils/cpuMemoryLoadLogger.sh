#!/bin/zsh

function startCpuMemoryLogger() {
  echo "startCpuMemoryLogger"
  # get taskmanagers
  echo "get taskmanagers"
  echo "(TMids=(\`curl ${flinkIP}:${flinkPort}/taskmanagers | jq '.taskmanagers[] | .id'\`))"
  TMids=(`curl ${flinkIP}:${flinkPort}/taskmanagers | jq '.taskmanagers[] | .id'`)

  # start cpuMemoryLogger.py
  echo "start cpuMemoryLogger.py"
  echo "(python cpuMemoryLogger.py ${flinkIP}:${flinkPort} ${TMids})"
  python cpuMemoryLogger.py ${flinkIP}:${flinkPort} ${TMids}
}

function stopCpuMemoryLogger() {
  pid=(`ps aux | grep "cpuMemoryLogger.py" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
  done
}