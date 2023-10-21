#!/bin/zsh

source ../config.sh
source ../utils/cpmanager.sh
source ../utils/logger.sh
source ../utils/flinkJob.sh
source ../utils/notifyEnd.sh

startCpID=1

if [ $# -ne 1 ]; then
  echo "Illegal Args"
  exit 1
fi

if [ $1 -eq 1 ]; then
  echo "****************** LR ******************"
  source ./config/configLR.sh
elif [ $1 -eq 2 ]; then
  echo "****************** Nexmark ******************"
  source ./config/configNexmark.sh
elif [ $1 -eq 3 ]; then
  echo "****************** NYC ******************"
  source ./config/configNYC.sh
elif [ $1 -eq 4 ]; then
  echo "****************** YSB ******************"
  source ./config/configYSB.sh
fi

logDir="${BIN_DIR}/checkCorrectness/log/${testName}"
logFile="${logDir}/all_baseline.log"

./dataLoader.sh 3 ${testName}-o
./dataLoader.sh 4 ${testName}-l

./dataLoader.sh 0 ${inputTopicName} ${inputFilePath}

cd ..
############ Baseline (ALL) ############
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
echo ${jobid} > jobid.txt

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

## Print max TS
cd ../checkCorrectness/log
echo "*** Print ts log (ALL) ***"
python findMaxWMFromTopic.py ${inputTopicName} all_baseline.log ${inputTopicName}

cd ..
./dataLoader.sh 1 ${inputTopicName} ${inputFilePath}

cd ..

############ Baseline (SPLIT) ############
logFile="${logDir}/split_baseline.log"
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

############ Replay from chk-i ############
prevLogFile=${logFile}
logFile="${logDir}/split_from_chk.log"
# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null

# submit job
echo "sumbit job"
./lineage.sh ${JAR_PATH} ${mainPath} ${jobid} ${startCpID} ${testName}-l

cd ../checkCorrectness
./dataLoader.sh 2 ${inputTopicName} ${inputFilePath} > /dev/null &

cd ../templates
## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${logFile})"
notifyEnd ${logFile}

## Notify all outputs were provided.
echo "*** Notify all outputs were provided ***"
echo "(notifyEnd ${prevLogFile})"
notifyEnd ${prevLogFile}

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

cd ../checkCorrectness/log
echo "*** Print ts log (SPLIT) ***"
python findMaxWMFromTopic.py ${inputTopicName} split_baseline.log ${inputTopicName}

echo "*** Cmp outputs ***"
python ${cmpPythonName} ${logDir}/all_baseline.log ${logDir}/split_baseline.log ${logDir}/split_from_chk.log ${startCpID} ${parseFlag}