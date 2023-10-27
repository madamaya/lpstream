#!/bin/zsh

cd $(dirname $0)

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  rm ./latency.metrics1*.log
  rm ./latency.metrics1*.txt
  rm ./LR.pdf
  rm ./Nexmark.pdf
  rm ./NYC.pdf
  rm ./YSB.pdf
  rm -rf ./figs
elif [ $1 = "fleshAll" ]; then
  rm ./latency.metrics1*.log
  rm ./latency.metrics1*.txt
  rm ./LR.pdf
  rm ./Nexmark.pdf
  rm ./NYC.pdf
  rm ./YSB.pdf
  rm -rf LR
  rm -rf Nexmark
  rm -rf NYC
  rm -rf YSB
  rm -rf figs
else
  echo "Illegal arguments (${1})"
  exit 1
fi
