#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# $1: jar path, $2: main path, $3: parallelism, $4: jobID, $5: chkID, $6: lineageTopicName, $7: latencyFlag
# type2
# $1: jar path, $2: main path, $3: parallelism, $4: jobID, $5: chkID, $6: lineageTopicName, $7: latencyFlag, $8: windowSize

if [ ${5} -eq 0 ]; then
CHK_ARG=""
else
CHK_ARG="-s ${L3_HOME}/data/checkpoints/_checkpoints/${4}/chk-${5}"
fi
windowSizeOption=""
latencyFlag=2
if [ $# -eq 7 ]; then
  latencyFlag=${7}
elif [ $# -eq 8 ]; then
  latencyFlag=${7}
  windowSizeOption=${8}
else
  echo "Illegal args (lineageReplay.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
${CHK_ARG} \
--parallelism 1 \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${3} \
--lineageMode Lineage \
${windowSizeOption} \
--lineageTopic ${6} \
--latencyFlag ${latencyFlag}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
