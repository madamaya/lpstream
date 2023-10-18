#!/bin/bash

source $(dirname $0)/../config.sh

# $1: jar path, $2: main path, $3: jobID, $4: chkID, $5: lineageTopicName
if [ ${4} -eq 0 ]; then
CHK_ARG=""
else
CHK_ARG="-s ${L3_HOME}/data/checkpoints/_checkpoints/${3}/chk-${4}"
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
${CHK_ARG} \
--parallelism 1 \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${parallelism} \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${5} \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
