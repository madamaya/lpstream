#!/bin/zsh

source ./config.sh

# $1: main path, $2: JobID, $3: timestampOfOutputTuple, $4: lineageTopicName, $5: maxWindowSize,
# $6: valueOfOutputTuple, $7: numOfSourceOp
echo "*** Identify checkpointID from which replay will be started ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $7)"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $7

echo "*** Start program to monitor specified output's lineage derivation ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor $3 $4 $6)"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor $3 $4 $6

echo "*** END: getLineage.sh ***"
