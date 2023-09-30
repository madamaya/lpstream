#!/bin/bash

EXE_CMD="../../flink-1.17.1/bin/flink run -d \
--parallelism 4 \
--class com.madamaya.l3stream.workflows.real.Real \
../../target/l3stream-1.0-SNAPSHOT.jar"

echo "${EXE_CMD}"
eval ${EXE_CMD}
