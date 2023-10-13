#!/bin/bash

EXE_CMD="../flink-1.17.1/bin/flink run -d \
--class com.madamaya.l3stream.samples.lr.alflink.wwkfcp.ReviewAnalysisReal \
../target/l3stream-1.0-SNAPSHOT.jar \
--statisticsFolder fuga \
--outputFile neko \
--sourcesNumber 1 \
--CpMServerIP localhost \
--CpMServerPort 10010 \
--lineageMode nonLineage"
echo "${EXE_CMD}"
eval ${EXE_CMD}
