#!/bin/bash

source $(dirname $0)/../../config.sh

# (-> must) $1: jar path, $2: main path, $3: parallelism, (-> optional) $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag
latencyFlag=1
queryNameOption=""
if [ $# -eq 5 ]; then
  queryNameOption="--queryName ${4}"
  latencyFlag=${5}
else
  echo "Illegal args (evaluation/tamplates/original.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${3} \
--class ${2} \
${1} \
${queryNameOption} \
--latencyFlag ${latencyFlag}"

echo "${EXE_CMD}"
eval ${EXE_CMD}