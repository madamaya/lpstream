#!/bin/zsh

source $(dirname $0)/../config.sh

pids=`ps aux | grep L3RealtimeLoader | grep -v grep | awk '{print $2}'`
for pid in ${pids[@]}
do
  kill ${pid}
done
