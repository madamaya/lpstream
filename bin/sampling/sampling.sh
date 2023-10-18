#!/bin/zsh

source ../config.sh
source logger.sh
source cpmanager.sh
source notifyEnd.sh
source flinkJob.sh

flinkHome="${FLINK_HOME}"
kafkaHome="${KAFKA_HOME}"
binHome="${BIN_DIR}"
checkpointHome="${CHECKPOINT_DIR}"
autocheckHome=`pwd`

if [ $# -eq 2 ]; then
testCaseName=$1
mainpath=$2
else
# manual
testCaseName="NYC"
fi

echo "redis-cli FLUSHDB"
redis-cli FLUSHDB

#**** premise: Flink, Kafka, and Redis cluster has been started. ****#
logDir="./log/${testCaseName}"

# Create outputs
logFile=${logDir}/${testCaseName}.log

## start kafka logger
echo "start Kafka logger"
startLogger ${testCaseName} ${checkpointID} ${logDir} ${logFile} > /dev/null

cd ..
./lineageManager.sh normal ${JAR_PATH} ${mainPath} ${}
## start checkpoint manager server
#echo "startCpMServer"
#startCpMServer ${binHome} ${checkpointHome}

## submit Flink job
#echo "submit Flink job"
#cd ${binHome}/${(L)testCaseName}/replayTest/
#cd ${binHome}/templates
#./nonlineage.sh $2 ${cpmIP} ${cpmPort} ${redisIP} ${redisPort} ${parallelism}

cd ${autocheckHome}

## return procedure, if kafka receive no tuple for N seconds
# sleep 10
notifyEnd ${logFile}

## get running Flink jobid
echo "getRunningJobID"
baselineJobid=`getRunningJobID`
echo ${baselineJobid} > jobid.txt

## cancel Flink job
echo "cancel Flink job"
cancelFlinkJobs ${flinkHome}

## stop checkpoint manager server
echo "stopCpMServer"
stopCpMServer

## stop Kafka logger
echo "stopLogger"
stopLogger

## sample target output tuple
echo "python sampling.py ${logFile} ${numOfTargetTuple}"
python sampling.py ${logFile} ${numOfTargetTuple}