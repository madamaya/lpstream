#!/bin/zsh

source ./config.sh
source ./utils/flinkJob.sh

# $1: JobID, $2: timestampOfOutputTuple, $3: outputTopicName, $4: maxWindowSize, $5: valueOfOutputTuple

echo "java -cp ../target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $4"
java -cp ../target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $4
echo "java -cp ../target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.ReplayMonitor $1 $2 $3 $5"
java -cp ../target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.ReplayMonitor $1 $2 $3 $5

echo "FINISH"

#echo "${flinkHome}/bin/flink cancel $1"
#${flinkHome}/bin/flink cancel $1