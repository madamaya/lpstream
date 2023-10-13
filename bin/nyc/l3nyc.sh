#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
--parallelism 4 \
--class com.madamaya.l3stream.workflows.nyc.L3NYC \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--maxParallelism 4 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode nonLineage \
--aggregateStrategy sortedPtr \

--latencyFlag 1"

echo "${EXE_CMD}"
eval ${EXE_CMD}
