#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/_checkpoints/39c8486953511225fedbcd9db111222c/chk-1 \
--parallelism 1 \
--allowNonRestoredState \
--class com.madamaya.l3stream.workflows.nexmark.L3Nexmark \
../../target/l3stream-1.0-SNAPSHOT.jar \
--sourcesNumber 2 \
--maxParallelism 4 \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${3} \
--latencyFlag 2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
