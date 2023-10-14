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
--class com.madamaya.l3stream.workflows.ysb.L3YSB \
${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--maxParallelism 4 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
