#!/bin/zsh

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  rm ./throughput.metrics1*.txt
  rm ./LR.pdf
  rm ./Nexmark.pdf
  rm ./NYC.pdf
  rm ./YSB.pdf
elif [ $1 = "fleshAll" ]; then
  rm ./throughput.metrics1*.txt
  rm -rf LR
  rm -rf Nexmark
  rm -rf NYC
  rm -rf YSB
else
  echo "Illegal arguments (${1})"
  exit 1
fi
