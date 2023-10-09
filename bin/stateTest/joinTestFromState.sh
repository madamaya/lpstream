#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/3ad4844ee02849de658dd8305f58b78a/chk-4 \
--parallelism 1 \
--class com.madamaya.l3stream.workflows.joinTest.JoinTest \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode nonLineage \
--aggregateStrategy sortedPtr \
--cpmProcessing \
--latencyFlag 1"

echo "${EXE_CMD}"
eval ${EXE_CMD}
