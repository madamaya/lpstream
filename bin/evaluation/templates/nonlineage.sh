#!/bin/bash

source $(dirname $0)/../../config.sh

# $1: jar path, $2: main path, $3: latencyFlag, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: parallelism
EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${5} \
--class ${2} \
${1} \
--lineageMode nonLineage \
--cpmProcessing \
--queryName ${4} \
--latencyFlag ${3}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
