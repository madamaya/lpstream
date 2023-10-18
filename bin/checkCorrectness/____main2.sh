#!/bin/zsh

source ../config.sh
source ../utils/cpmanager.sh
source ../utils/logger.sh
source ../utils/flinkJob.sh
source ../utils/notifyEnd.sh

testName="NYC"
mainPath="com.madamaya.l3stream.workflows.nyc.L3NYC"

logDir="/Users/yamada-aist/workspace/l3stream/bin/normalAndLineage/log/${testName}"
logFile="${logDir}/split_baseline.log"

cd ..

### Baseline
# start CpMServer
echo "start CpMServer"
./startCpMServer.sh > /dev/null &

# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-o > /dev/null

# submit job
cd ./templates
echo "sumbit job"
./nonlineage.sh ${JAR_PATH} ${mainPath}

## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${logFile})"
notifyEnd ${logFile}

jobid=`getRunningJobID`

### Replay from chk-i
prevLogFile=${logFile}
logFile="${logDir}/split_from_chk.log"
# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null

# submit job
echo "sumbit job"
./lineage.sh ${JAR_PATH} ${mainPath} ${jobid} 1 ${testName}-l

cd ../normalAndLineage
./dataLoader.sh 2 > /dev/null &

cd ../templates
## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${logFile})"
notifyEnd ${logFile}

## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${prevLogFile})"
notifyEnd ${prevLogFile}
OUT-
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
