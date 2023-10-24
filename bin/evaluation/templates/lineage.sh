#!/bin/bash

source $(dirname $0)/../config.sh

# $1: jar path, $2: main path, $3: latencyFlag, $4: lineageTopic, $5: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $6: parallelism
EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism 1 \
--allowNonRestoredState \
--class ${2} \
${1} \
--maxParallelism ${6} \
--lineageMode Lineage \
--aggregateStrategy sortedPtr \
--lineageTopic ${4} \
--queryName ${4} \
--latencyFlag ${3}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
