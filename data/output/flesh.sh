#!/bin/zsh

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  ./cpu-memory/flesh.sh flesh
  ./latency/flesh.sh flesh
  ./throughput/flesh.sh flesh
elif [ $1 = "fleshAll" ]; then
  ./cpu-memory/flesh.sh fleshAll
  ./latency/flesh.sh fleshAll
  ./throughput/flesh.sh fleshAll
  rm -rf ./throughput/17*
else
  echo "Illegal arguments (${1})"
  exit 1
fi
