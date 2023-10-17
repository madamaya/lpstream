#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/084ad4ffd06e617f6368500872d6e555/chk-1 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.nyc.L3NYC \
../../target/l3stream-1.0-SNAPSHOT.jar \
--sourcesNumber 1 \
--maxParallelism 4 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${3} \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
