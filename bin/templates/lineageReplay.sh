#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# $1: jar path, $2: main path, $3: parallelism, $4: jobID, $5: chkID, $6: lineageTopicName, $7: latencyFlag
if [ ${5} -eq 0 ]; then
CHK_ARG=""
startingOffset="earliest"
else
CHK_ARG="-s ${L3_HOME}/data/checkpoints/${4}/chk-${5}"
startingOffset="latest"
fi
latencyFlag=2
aggregateStrategy=""
if [ $# -eq 7 ]; then
  latencyFlag=${7}
else
  echo "Illegal args (lineageReplay.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
${CHK_ARG} \
--parallelism ${3} \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${3} \
--lineageMode Lineage \
--lineageTopic ${6} \
--latencyFlag ${latencyFlag} \
--startingOffset ${startingOffset}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
