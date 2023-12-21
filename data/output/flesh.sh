#!/bin/zsh

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  ./cpu-memory/flesh.sh flesh
  ./latency/metrics1/flesh.sh flesh
  ./latency/metrics2/flesh.sh flesh
  ./throughput/metrics1/flesh.sh flesh
  ./throughput/metrics2/flesh.sh flesh
  ./metrics34/flesh.sh flesh
elif [ $1 = "fleshAll" ]; then
  ./cpu-memory/flesh.sh fleshAll
  ./latency/metrics1/flesh.sh fleshAll
  ./latency/metrics2/flesh.sh fleshAll
  ./throughput/metrics1/flesh.sh fleshAll
  ./throughput/metrics2/flesh.sh fleshAll
  rm -rf ./throughput/170*
  ./metrics34/flesh.sh fleshAll
else
  echo "Illegal arguments (${1})"
  exit 1
fi
