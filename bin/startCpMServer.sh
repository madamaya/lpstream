#!/bin/bash

source $(dirname $0)/config.sh

EXE_CMD="java -cp ${JAR_PATH} com.madamaya.l3stream.cpstore.CpManagerServer \
--checkpointDir ${CHECKPOINT_DIR} \
--CpMServerIP ${cpmIP} \
--CpMServerPort ${cpmPort}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
