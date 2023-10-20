#!/bin/zsh

source ./config.sh

source ./workflowConf/configLR.sh
#source ./workflowConf/configNexmark.sh
#source ./workflowConf/configNYC.sh
#source ./workflowConf/configYSB.sh

source ./utils/logger.sh
source ./utils/notifyEnd.sh
source ./utils/flinkJob.sh
source ./utils/cpmanager.sh

# Call Lineage Manager (normal mode)
# This driver emulates the call from "Program Converter" to "Lineage Manager".
# Therefore, "ChkDir, CpMServerIP, CpMServerPort, RedisIP, and RedisPort" have been already configured.
# Note that, to automatically select lineage target tuple, all output are stored in "${L3_HOME}/date/log/${testName}/xxx.log"

# Define log file
echo "*** Define log file ***"
echo "logDir=\"${L3_HOME}/data/log/${(L)testName}\""
logDir="${L3_HOME}/data/log/${(L)testName}"
echo "logFile=\"${logDir}/${testName}.log\""
logFile="${logDir}/${testName}.log"

## Initialize redis
echo "*** Initialize redis ***"
echo "(redis-cli FLUSHDB)"
redis-cli FLUSHDB

## Start kafka logger
echo "*** Start Kafka logger ***"
echo "(startKafkaLogger ${logDir} ${logFile} ${outputTopicName})"
startKafkaLogger ${logDir} ${logFile} ${outputTopicName} > /dev/null

## start checkpoint manager server
echo "*** Start Checkpoint Management Server ***"
echo "(./startCpMServer.sh > /dev/null &)"
./startCpMServer.sh > /dev/null &

## submit Flink job
cd ${BIN_DIR}/templates
echo "*** Submit Flink job ***"
echo "(./nonlineage.sh ${JAR_PATH} ${mainPath})"
./nonlineage.sh ${JAR_PATH} ${mainPath}

cd ${BIN_DIR}
echo "*** Get jobid ***"
echo "(jobid=\`getRunningJobID\`)"
jobid=`getRunningJobID`

## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${logFile})"
notifyEnd ${logFile}

## Cancel Flink job
echo "*** Cancel Flink job ***"
echo "(cancelFlinkJobs)"
cancelFlinkJobs

## Stop CpMServerManager
echo "*** Stop CpMServerManager ***"
echo "(stopCpMServer)"
stopCpMServer

## Stop kafka logger
echo "*** Stop kafka logger ***"
echo "(stopLogger)"
stopLogger

# Call Lineage Manager (lineage mode)
# This driver emulates the call from "User, seeing flink output" to "Lineage Manager".

## Decide and write target outputs for lineage randomly in a log file
echo "*** Decide target output for lineage randomly ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.Sampling ${logFile} ${numOfSamples})"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.Sampling ${logFile} ${numOfSamples}

## Read target outputs
FILE="${logFile}.target.txt"
while read LINE
do
  outputValue=`echo ${LINE} | jq '.OUT'`
  outputTs=`echo ${LINE} | jq '.TS' | sed -e 's/"//g'`

  ## Start Lineage Manager with normal mode
  ./lineageManager.sh ${JAR_PATH} ${mainPath} ${jobid} ${outputTs} ${outputValue} ${maxWindowSize} ${lineageTopicName}
done < ${FILE}