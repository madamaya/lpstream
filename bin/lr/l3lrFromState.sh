#!/bin/bash

source $(dirname $0)/../config.sh

if [ $2 -eq 0 ]; then
CHK_ARG=""
else
CHK_ARG="-s ${L3_HOME}/data/checkpoints/_checkpoints/${1}/chk-${2}"
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
${CHK_ARG} \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.lr.L3LR \
${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar \
--sourcesNumber 1 \
--maxParallelism 4 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${3} \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
