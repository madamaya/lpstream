#!/bin/bash

source $(dirname $0)/../config.sh

# $1: main path, $2: jobID, $3: chkID, $4: parallelism of nonlienage mode, $5: lineageTopicName
if [ ${3} -eq 0 ]; then
CHK_ARG=""
else
CHK_ARG="-s ${L3_HOME}/data/checkpoints/_checkpoints/${2}/chk-${3}"
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
${CHK_ARG} \
--parallelism 1 \
--allowNonRestoredState \
--class ${1} \
${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar \
--maxParallelism ${4} \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${5} \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
