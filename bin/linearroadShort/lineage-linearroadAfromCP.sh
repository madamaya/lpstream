#!/bin/bash

EXE_CMD="../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/af8d945a9401d86607f8bbaffa282f5d/chk-20 \
--parallelism 2 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.linearroad.noprovenance.wqs.LinearRoadAccident \
../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--aggregateStrategy sortedPtr \
--lineageMode Lineage \
--latencyFlag 0"
echo "${EXE_CMD}"
eval ${EXE_CMD}
