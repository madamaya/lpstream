#!/bin/bash

source $(dirname $0)/../../config.sh

# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag, $6: lineageTopic
EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism 1 \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${3} \
--lineageMode Lineage \
--lineageTopic ${6} \
--queryName ${4} \
--latencyFlag ${5}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
