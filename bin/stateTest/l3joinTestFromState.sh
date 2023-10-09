#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/0d8ad7a7f9e327fa358643cf7155e712/chk-4 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.joinTest.L3JoinTest \
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
