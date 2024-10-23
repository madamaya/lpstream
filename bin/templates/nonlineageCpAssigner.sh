#!/bin/bash

source $(dirname $0)/../config.sh

# type1
# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: inputDataSize
# type3
# (-> must) $1: jar path, $2: main path, $3: parallelism, $4: queryName (${L3_HOME}/data/output/throughput/~~~, e.g., metrics1/YSB), $5: latencyFlag, $6: inputDataSize

latencyFlag=1
queryNameOption=""
inputDataSize=""
if [ $# -eq 6 ]; then
  queryNameOption="--queryName ${4}"
  latencyFlag=${5}
  inputDataSize="--dataSize ${6}"
elif [ $# -eq 4 ]; then
  inputDataSize="--dataSize ${4}"
else
  echo "Illegal args (evaluation/tamplates/nonlineageCpAssigner.sh)"
  exit 1
fi

EXE_CMD="${FLINK_HOME}/bin/flink run -d \
--parallelism ${3} \
--class ${2} \
${1} \
--lineageMode nonLineage \
${queryNameOption} \
${inputDataSize} \
--invokeCpAssigner \
--latencyFlag ${latencyFlag}"

echo "${EXE_CMD}"
eval ${EXE_CMD}
