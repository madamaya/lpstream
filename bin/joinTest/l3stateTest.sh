#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
--parallelism 1 \
--class com.madamaya.l3stream.workflows.stateTest.L3StateTest \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--cpmProcessing \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
