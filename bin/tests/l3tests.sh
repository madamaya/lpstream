#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
--parallelism 1 \
--class com.madamaya.l3stream.tests.RichFlatMapTest \
../../target/l3stream-1.0-SNAPSHOT.jar"
echo "${EXE_CMD}"
eval ${EXE_CMD}
