#!/bin/bash

source $(dirname $0)/../config.sh

# (-> must) $1: jar path, $2: parallelism, $3: queryName, $4: logFilePath

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${2} \
--class com.madamaya.l3stream.workflows.utils.LatencyCalc \
${1} \
${3} \
${4}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
