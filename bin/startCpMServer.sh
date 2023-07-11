#!/bin/bash

if [ $# -ne 2 ];then
    echo "Usage: $0 {ip} {CheckpointDir}";
    exit 1
fi

EXE_CMD="java -cp ../target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.cpstore.CpManagerServer \
--ip $1
--checkpointDir $2"

echo "${EXE_CMD}"
eval ${EXE_CMD}
