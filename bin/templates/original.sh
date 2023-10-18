#!/bin/bash

source $(dirname $0)/../config.sh

# $1: jar path, $2: main path
EXE_CMD="${FLINK_HOME}/flink-1.17.1/bin/flink run -d \
--parallelism ${parallelism} \
--class ${2} \
${1}";

echo "${EXE_CMD}"
eval ${EXE_CMD}
