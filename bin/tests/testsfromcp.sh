#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
-s /Users/yamada-aist/workspace/l3stream/data/checkpoints/d7abfb8169ca248da41bc4ba0344c0bc/chk-4 \
--parallelism 4 \
--class com.madamaya.l3stream.tests.RichMapTest \
../../target/l3stream-1.0-SNAPSHOT.jar"
echo "${EXE_CMD}"
eval ${EXE_CMD}
