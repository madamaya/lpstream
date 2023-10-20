#!/bin/zsh

source ./config.sh

# $1: jar path $2: main path, $3: JobID, $4: timestampOfOutputTuple
# $5: lineageTopicName, $6: maxWindowSize, $7: valueOfOutputTuple, $8: numOfSourceOp
echo "*** Identify checkpointID from which replay will be started ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $6 $8)"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $6 $8

echo "*** Start program to monitor specified output's lineage derivation ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor $4 $5 $7)"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor $4 $5 $7

echo "*** END: getLineage.sh ***"
