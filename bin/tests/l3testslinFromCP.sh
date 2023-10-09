#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/c6097fe6eb86d1ca54145a38b1ab0b23/chk-5 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.tests.L3RichFlatMapTest \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--latencyFlag 2"
echo "${EXE_CMD}"
eval ${EXE_CMD}
