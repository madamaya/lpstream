#!/bin/zsh

source logger.sh
source cpmanager.sh
source notifyEnd.sh
source flinkJob.sh

l3Home="/Users/yamada-aist/workspace/l3stream"
flinkHome="${l3Home}/flink-1.17.1"
kafkaHome="${l3Home}/kafka_2.12-3.4.1"
binHome="${l3Home}/bin"
checkpointHome="${l3Home}/data/checkpoints"
autocheckHome=`pwd`

testCaseName="Nexmark"

echo "redis-cli FLUSHDB"
redis-cli FLUSHDB

#**** premise: Flink, Kafka, and Redis cluster has been started. ****#
logDir="./log/${testCaseName}"

# BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE
# BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE
# BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE
# BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE
# BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE*BASELINE
# Create baseline outputs
checkpointID=0
logFile=${logDir}/${testCaseName}${checkpointID}.log

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

## cancel Flink job
echo "cancel Flink job"
cancelFlinkJobs ${flinkHome}

## stop checkpoint manager server
echo "stopCpMServer"
stopCpMServer

## stop Kafka logger
echo "stopLogger"
stopLogger


# REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY
# REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY
# REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY
# REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY
# REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY*REPLAY
# Create replay outputs
cpFiles=(`ls -1 ${checkpointHome}/_checkpoints/${baselineJobid}`)
#for idx in `seq 1 ${#cpFiles[*]}`
for idx in `seq 1 1`
do
  checkpointID=${idx}
  logFile=${logDir}/${testCaseName}${checkpointID}.log

  ## start kafka logger
  echo "start Kafka logger"
  startLogger ${testCaseName} ${checkpointID} ${logDir} ${logFile} > /dev/null

  ## submit Flink job
  echo "submit Flink job"
  echo "REPLAY from ${baselineJobid}'s state"
  cd ${binHome}/${(L)testCaseName}/replayTest/
  ./l3${(L)testCaseName}FromState.sh ${baselineJobid} ${checkpointID}

  cd ${autocheckHome}

  ## return procedure, if kafka receive no tuple for N seconds
  # sleep 10
  notifyEnd ${logFile}

  ## cancel Flink job
  echo "cancel Flink job"
  cancelFlinkJobs ${flinkHome}

  ## stop Kafka logger
  echo "stopLogger"
  stopLogger
done