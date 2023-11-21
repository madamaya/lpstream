#!/bin/zsh

source $(dirname $0)/../config.sh

filePath=$1
qName=$2
topic=$3
paral=$4

echo "java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderTest ${filePath} ${qName} ${topic} ${paral}"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.L3RealtimeLoaderTest ${filePath} ${qName} ${topic} ${paral}
