#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/b099b49f7a6106c0d9d805ee4048320d/chk-27 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.linearroad.noprovenance.qs.provenance.LinearRoadCombined \
../../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--cpmProcessing"
echo "${EXE_CMD}"
eval ${EXE_CMD}
