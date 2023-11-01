#!/bin/zsh

source $(dirname $0)/../config.sh

topic=$1
checkInterval=$2
query=$3

#query="LR"
#query="Nexmark"
#query="NYC"
#query="YSB"

mainPath="com.madamaya.l3stream.workflows.${(L)query}.${query}"
# Run
echo "*** Run ***"
echo "(../templates/original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/baseline 0)"
../templates/original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/baseline 0

echo "*** Data size test (topic=${topic}, parallelism=${parallelism}, checkInterval=${checkInterval}) ***"
echo "java -cp ${JAR_PATH} com.madamaya.l3stream.utils.IdentifyEnd ${topic} ${parallelism} ${checkInterval}"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.IdentifyEnd ${topic} ${parallelism} ${checkInterval}
