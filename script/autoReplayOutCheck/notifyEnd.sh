#!/bin/zsh

# $1: logFile
function notifyEnd() {
  logFile=$1

  prev=-1
  current=0
  while :
  do
    current=`wc -l ${logFile} | awk '{print $1}'`
    if [ ${prev} -eq ${current} ]; then
      echo "prev=${prev}, current=${current} -> break"
      break
    fi
    echo "prev=${prev}, current=${current}"
    prev=${current}
    sleep 60
  done
}