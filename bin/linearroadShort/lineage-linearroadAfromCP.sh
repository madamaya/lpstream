#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/48e4f5a6d67ca36c051c67e3d4cbeb17/chk-8 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.linearroad.noprovenance.wqs.LinearRoadAccidentShort \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--aggregateStrategy sortedPtr \
--lineageMode Lineage \
--latencyFlag 2"
echo "${EXE_CMD}"
eval ${EXE_CMD}
