#!/bin/zsh

cd $(dirname $0)

if [ $# -ne 1 ]; then
  echo "Illegal arguments"
  exit 1
fi

if [ $1 = "flesh" ]; then
  rm -rf results
elif [ $1 = "fleshAll" ]; then
  rm -rf results
  rm -rf LR
  rm -rf Nexmark*
  rm -rf NYC
  rm -rf YSB
else
  echo "Illegal arguments (${1})"
  exit 1
fi
