#!/bin/zsh

source $(dirname $0)/../config.sh

filePath=$1
qName=$2
topic=$3
paral=$4
throughput=$5
granularity=$6

echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoader ${filePath} ${qName} ${topic} ${paral} ${throughput} ${granularity})"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoader ${filePath} ${qName} ${topic} ${paral} ${throughput} ${granularity}
