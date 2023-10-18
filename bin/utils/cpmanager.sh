#!/bin/zsh

source $(dirname $0)/../config.sh

function startCpMServerFunc() {
  cd ${binHome}
  echo "./startCpMServer.sh ${cpmIP} ${cpmPort} ${checkpointHome}"
  ./startCpMServer.sh ${cpmIP} ${cpmPort} ${checkpointHome} > /dev/null &
  echo "./startCpMServer.sh"
  ./startCpMServer.sh > /dev/null &
}

function stopCpMServer() {
  pid=(`ps aux | grep "CpManagerServer" | grep -v grep | awk '{print $2}'`)
  for p in ${pid[@]}
  do
    echo "kill ${p}"
    kill ${p}
  done
}
