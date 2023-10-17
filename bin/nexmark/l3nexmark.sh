#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
--parallelism 4 \
--class com.madamaya.l3stream.workflows.nexmark.L3Nexmark \
../../target/l3stream-1.0-SNAPSHOT.jar \
--sourcesNumber 2 \
--maxParallelism 4 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--RedisIP localhost \
--RedisPort 6379 \
--lineageMode nonLineage \
--aggregateStrategy sortedPtr \
--cpmProcessing \
--latencyFlag 1"

echo "${EXE_CMD}"
eval ${EXE_CMD}
