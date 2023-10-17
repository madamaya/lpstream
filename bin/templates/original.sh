#!/bin/bash

source $(dirname $0)/../config.sh

# $1: main path, $2: parallelism
EXE_CMD="${FLINK_HOME}/flink-1.17.1/bin/flink run -d \
--parallelism ${2} \
--class ${1} \
../../target/l3stream-1.0-SNAPSHOT.jar"

echo "${EXE_CMD}"
eval ${EXE_CMD}
