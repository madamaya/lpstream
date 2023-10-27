#!/bin/zsh

source ./bin/config.sh

if [ $# -ne 1 ]; then
  echo "$#"
  echo "Illegal Arguments."
  exit 1
fi

if [ $1 = "m1" ]; then
  cd ./bin/evaluation
  ./metrics1.sh

  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 10
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 10
elif [ $1 = "m2" ]; then
  cd ./bin/evaluation
  ./metrics2.sh

  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 10
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 10
elif [ $1 = "m34" ]; then
  cd ./bin
  ./LMUserDriver.sh 1
  ./LMUserDriver.sh 2
  ./LMUserDriver.sh 3
  ./LMUserDriver.sh 4

  cd ${L3_HOME}/data/output/metrics34
  python metrics34.py

  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 10
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 10
elif [ $1 = "debug" ]; then
  cd ./bin/evaluation

  echo "*** DEBUG ***"
  ${FLINK_HOME}/bin/stop-cluster.sh
  sleep 10
  ${FLINK_HOME}/bin/start-cluster.sh
  sleep 10
else
  echo "Illegal Arguments"
  exit 1
fi
