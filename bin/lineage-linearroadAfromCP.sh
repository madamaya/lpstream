#!/bin/bash

EXE_CMD="../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/598b3c81901648a2debd7293c9cf9e25/chk-20 \
--parallelism 4 \
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
