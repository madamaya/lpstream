#!/bin/zsh

source $(dirname $0)/../config.sh

topic=$1
checkInterval=$2

echo "*** Data size test (topic=${topic}, parallelism=${parallelism}, checkInterval=${checkInterval}) ***"
echo "java -cp ${JAR_PATH} com.madamaya.l3stream.utils.IdentifyEnd ${topic} ${parallelism} ${checkInterval}"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.IdentifyEnd ${topic} ${parallelism} ${checkInterval}
