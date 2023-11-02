#!/bin/zsh

sleepTimeNotifyEnd=90
sleepTimeNotifyMonitorEnd=30
# $1: logFile
function notifyEnd() {
  logFile=$1

  prev=-1
  current=0
  while :
  do
    current=`wc -l ${logFile} | awk '{print $1}'`
    if [ ${prev} -eq ${current} ]; then
      echo "prev=${prev}, current=${current} -> break" `date`
      break
    fi
    echo "prev=${prev}, current=${current}" `date`
    prev=${current}
    sleep ${sleepTimeNotifyEnd}
  done
}

function notifyReplayMonitorEnd() {
  isRun=`ps aux | grep ReplayMonitor | grep -v grep | wc -l | awk '{print $1}'`
  while :
  do
    if [ ${isRun} -eq 0 ]; then
      break
    fi
    sleep ${sleepTimeNotifyMonitorEnd}
    isRun=`ps aux | grep ReplayMonitor | grep -v grep | wc -l | awk '{print $1}'`
  done
}