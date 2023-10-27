#!/bin/zsh

source ./config.sh
source ./utils/flinkJob.sh
source ./utils/notifyEnd.sh

mode=$1

if [ $# -ne 9 ]; then
  echo "Illegal Arguments (lineageManager.sh)"
  echo $1 $2 $3 $4 $5 $6 $7 $8 $9
  exit 1
fi
# $1: jarPath, $2: mainPath, $3: jobid, $4: outputTs, $5: outputValue, $6: maxWsize, $7: lineageTopicName, $8: experimentName $9: windowSize
jarPath=$1
mainPath=$2
jobid=$3
outputTs=$4
outputValue=$5
maxWindowSize=$6
lineageTopicName=$7
experimentName=$8
windowSize=$9
# define numOfSourceOp
if [[ ${mainPath} == *Nexmark* ]]; then
  numOfSourceOp=2
else
  numOfSourceOp=1
fi

echo "*** Start lineage derivation ***"

# CNFM: merics3,4がちゃんとなってるか確認
echo "*** Start program to monitor specified output's lineage derivation ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor ${outputTs} ${lineageTopicName} ${outputValue} ${experimentName} ${windowSize} &)"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.ReplayMonitor ${outputTs} ${lineageTopicName} ${outputValue} ${experimentName} ${windowSize} &

echo "*** Identify checkpointID from which replay will be started ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay ${jarPath} ${mainPath} ${jobid} ${outputTs} ${lineageTopicName} ${maxWindowSize} ${numOfSourceOp} ${experimentName} ${windowSize})"
java -cp ${JAR_PATH} com.madamaya.l3stream.getLineage.TriggerReplay ${jarPath} ${mainPath} ${jobid} ${outputTs} ${lineageTopicName} ${maxWindowSize} ${numOfSourceOp} ${experimentName} ${windowSize}

notifyReplayMonitorEnd
