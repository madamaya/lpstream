#!/bin/zsh

cd $(dirname $0)

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  rm ./metrics3*.txt
  rm ./metrics4*.txt
elif [ $1 = "fleshAll" ]; then
  rm ./metrics3*.txt
  rm ./metrics4*.txt
  rm -rf LR
  rm -rf Nexmark
  rm -rf NYC
  rm -rf YSB
else
  echo "Illegal arguments (${1})"
  exit 1
fi
