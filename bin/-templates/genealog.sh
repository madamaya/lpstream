#!/bin/bash

source $(dirname $0)/../config.sh

# $1: jar path, $2: main path, $3: latencyFlag
latencyFlag=1
if [ $# -eq 3 ]; then
  latencyFlag=${3}
fi
EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${parallelism} \
--class ${2} \
${1} \
--latencyFlag ${latencyFlag}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
