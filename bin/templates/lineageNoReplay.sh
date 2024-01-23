#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# $1: jar path, $2: main path, $3: parallelism, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag, $6: lineageTopic, $7: aggregateStrategy, $8: inputDataSize

if [ $# -ne 8 ]; then
  echo "Illegal args (lineageNoReplay.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${3} \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${3} \
--lineageMode Lineage \
--lineageTopic ${6} \
--queryName ${4} \
--aggregateStrategy ${7} \
--dataSize ${8}
--latencyFlag ${5}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
