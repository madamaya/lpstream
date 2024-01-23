#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: aggregateStrategy, $5: inputDataSize
# type2
# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag, $6: aggregateStrategy, $7: inputDataSize

queryNameOption=""
latencyFlag=1
aggregateStrategy=""
inputDataSize=""
if [ $# -eq 7 ]; then
  queryNameOption="--queryName ${4}"
  latencyFlag=${5}
  aggregateStrategy=${6}
  inputDataSize="--dataSize ${7}"
elif [ $# -eq 5 ]; then
  aggregateStrategy=${4}
  inputDataSize="--dataSize ${5}"
else
  echo "Illegal args (evaluation/tamplates/genealog.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${3} \
--class ${2} \
${1} \
${queryNameOption} \
${inputDataSize} \
--aggregateStrategy ${aggregateStrategy} \
--latencyFlag ${latencyFlag}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
