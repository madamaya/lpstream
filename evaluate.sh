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
  throughput=50000
  ./latency.sh ${throughput} | tee latency.log
  restartFlinkCluster
elif [ $1 = "throughput" ]; then
  cd ./bin/evaluation
  ./throughput.sh | tee throughput.log
  restartFlinkCluster
elif [ $1 = "m34" ]; then
  cd ./bin
  ./LMUserDriver.sh 1 2>&1 | tee LMUserDriver1.log
  restartFlinkCluster
  ./LMUserDriver.sh 2 2>&1 | tee LMUserDriver2.log
  restartFlinkCluster
  ./LMUserDriver.sh 3 2>&1 | tee LMUserDriver3.log
  restartFlinkCluster
  ./LMUserDriver.sh 4 2>&1 | tee LMUserDriver4.log
  restartFlinkCluster

  cd ${L3_HOME}/data/output/metrics34
  python metrics34.py 1

  #cd ${L3_HOME}/bin
  #./LMUserDriver.sh 1 2
  #./LMUserDriver.sh 2 2
  #./LMUserDriver.sh 3 2
  #./LMUserDriver.sh 4 2

  #cd ${L3_HOME}/data/output/metrics34
  #python metrics34.py 2
  #${FLINK_HOME}/bin/stop-cluster.sh
  #sleep 10
  #${FLINK_HOME}/bin/start-cluster.sh
  #sleep 10
else
  echo "Illegal Arguments"
  exit 1
fi
