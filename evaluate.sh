#!/bin/zsh

source ./bin/config.sh
source ./bin/utils/flinkJob.sh

if [ $# -ne 1 ]; then
  echo "$#"
  echo "Illegal Arguments."
  exit 1
fi

if [ $1 = "latency" ]; then
  cd ./bin/evaluation
  ./latency.sh |& tee latency.log
elif [ $1 = "throughput" ]; then
  cd ./bin/evaluation/thConf
  python confGen.py 1
  cd ..
  ./throughput.sh |& tee throughput.log
  cd ./thConf
  python confGen.py ../finishedComb.csv.*
  ./throughput.sh |& tee throughput.log
  #./throughput.sh |& tee throughput.log
elif [ $1 = "duration" ]; then
  cd ./bin/getLineage
  ./lineageDuration.sh
else
  echo "Illegal Arguments"
  exit 1
fi
