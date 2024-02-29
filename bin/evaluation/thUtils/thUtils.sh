#!/bin/zsh

source $(dirname $0)/../../config.sh

function isValid() {
  # $1: query, $2: approach, $3: inputSize
  i_query=${1}
  i_approach=${2}
  i_inputSize=${3}
  num=`cat finishedComb.csv | grep "unstable,${i_query},${i_approach},${i_inputSize}," | wc -l | awk '{print $1}'`

  echo "=================================================="
  echo "isValid:"
  echo "    query:" ${i_query} ", approach:" ${i_approach} ", inputSize:" ${i_inputSize} ", num:" ${num}
  echo "=================================================="
  return $num
}

function updateValid() {
    # $1: queries, $2: approaches, $3: inputSizes, $4: inputRate
    i_queries=${1}
    i_approaches=${2}
    i_inputSizes=${3}
    i_inputRate=${4}
    echo "=================================================="
    echo "updateValid"
    echo "    queries:" ${i_queries}
    echo "    approaches:" ${i_approaches}
    echo "    inputSizes:" ${i_inputSizes}
    echo "    inputRate:" ${i_inputRate}
    echo "=================================================="

    echo "python updateValid.py "${i_queries}" "${i_approaches}" "${i_inputSizes}" ${i_inputRate} ${L3_HOME}/data/output"
    python updateValid.py "${i_queries}" "${i_approaches}" "${i_inputSizes}" ${i_inputRate} ${L3_HOME}/data/output
}