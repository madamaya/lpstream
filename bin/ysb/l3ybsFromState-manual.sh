#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/f5cf11e96d0915e1b18d21271e0ffc36/chk-12 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.ysb.L3YSB \
../../target/l3stream-1.0-SNAPSHOT.jar \
--sourcesNumber 1 \
--maxParallelism 4 \
--lineageMode nonLineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${3} \
--latencyFlag 1"

echo "${EXE_CMD}"
eval ${EXE_CMD}
