#!/bin/zsh

source ../config.sh
source logger.sh
source cpmanager.sh
source notifyEnd.sh
source flinkJob.sh

flinkHome="${L3_HOME}/flink-1.17.1"
kafkaHome="${L3_HOME}/kafka_2.12-3.4.1"
binHome="${L3_HOME}/bin"
checkpointHome="${L3_HOME}/data/checkpoints"
autocheckHome=`pwd`

if [ $# -ne 1 ]; then
testCaseName=$1
else
# manual
testCaseName="NYC"
fi


#echo "redis-cli FLUSHDB"
#redis-cli FLUSHDB

#**** premise: Flink, Kafka, and Redis cluster has been started. ****#
logDir="./log/${testCaseName}"

# Create outputs
logFile=${logDir}/${testCaseName}.log

## start kafka logger
echo "start Kafka logger"
startLogger ${testCaseName} ${checkpointID} ${logDir} ${logFile} > /dev/null

## start checkpoint manager server
echo "startCpMServer"
startCpMServer ${binHome} ${checkpointHome}

## submit Flink job
echo "submit Flink job"
cd ${binHome}/${(L)testCaseName}/replayTest/
./l3${(L)testCaseName}.sh

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