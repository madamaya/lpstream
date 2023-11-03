#!/bin/zsh

source ./bin/config.sh
source ./bin/utils/flinkJob.sh

if [ $# -ne 1 ]; then
  echo "$#"
  echo "Illegal Arguments."
  exit 1
fi

if [ $1 = "m1" ]; then
  cd ./bin/evaluation
  ./metrics1.sh
elif [ $1 = "m2" ]; then
  cd ./bin/evaluation
  ./metrics2.sh
elif [ $1 = "m34" ]; then
  cd ./bin
  ./LMUserDriver.sh 1
  restartTMifNeeded
  ./LMUserDriver.sh 2
  restartTMifNeeded
  ./LMUserDriver.sh 3
  restartTMifNeeded
  ./LMUserDriver.sh 4
  restartTMifNeeded

  cd ${L3_HOME}/data/output/metrics34
  python metrics34.py 1
  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 10
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 10

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
elif [ $1 = "debug" ]; then
  cd ./bin/evaluation

  echo "*** DEBUG ***"
  #${FLINK_HOME}/bin/stop-cluster.sh
  #sleep 10
  #${FLINK_HOME}/bin/start-cluster.sh
  #sleep 10
else
  echo "Illegal Arguments"
  exit 1
fi
