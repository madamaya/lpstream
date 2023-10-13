#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/54a9860ba618d303baf1665fb2f0178d/chk-32 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.linearroad.noprovenance.wqs.LinearRoadAccident \
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

# Lineage→Lineageで結果だけでも一緒だったりしないか？