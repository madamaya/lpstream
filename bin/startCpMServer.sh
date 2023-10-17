#!/bin/bash

source ./config.sh

if [ $# -ne 3 ];then
    echo "Usage: $0 {CpMServerIP} {CpMServerPort} {CheckpointDir}";
    exit 1
fi

EXE_CMD="java -cp ${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.cpstore.CpManagerServer \
--CpMServerIP $1
--CpMServerPort $2
--checkpointDir $3"

echo "${EXE_CMD}"
eval ${EXE_CMD}
