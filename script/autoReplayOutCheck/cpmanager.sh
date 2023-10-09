#!/bin/zsh

function startCpMServer() {
  binHome=$1
  checkpointHome=$2
  cd ${binHome}
  echo "./startCpMServer.sh localhost ${checkpointHome}"
  ./startCpMServer.sh "localhost" ${checkpointHome} > /dev/null &
}

function stopCpMServer() {
  pid=(`ps aux | grep "CpManagerServer" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
  done
}
