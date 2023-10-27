#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# (-> must) $1: jar path, $2: main path, $3: parallelism
# type2
# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag

queryNameOption=""
latencyFlag=1
if [ $# -eq 5 ]; then
  queryNameOption="--queryName ${4}"
  latencyFlag=${5}
elif [ $# -ne 3 ]; then
  echo "Illegal args (evaluation/tamplates/genealog.sh)"
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
