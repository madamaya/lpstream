#!/bin/bash

source $(dirname $0)/../config.sh

# $1: main path, $2: CpMServerIP, $3: CpMServerPort, $4: RedisIP, $5: RedisPort, $6: Parallelism
EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${6} \
--class ${1} \
${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar \
--maxParallelism ${6} \
--CpMServerIP ${2} \
--CpMServerPort ${3} \
--RedisIP ${4} \
--RedisPort ${5} \
--lineageMode nonLineage \
--aggregateStrategy sortedPtr \
--cpmProcessing \
--latencyFlag 1"

echo "${EXE_CMD}"
eval ${EXE_CMD}
