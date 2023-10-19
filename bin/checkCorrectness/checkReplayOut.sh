#!/bin/zsh

source ../config.sh
source ../utils/cpmanager.sh
source ../utils/logger.sh
source ../utils/flinkJob.sh
source ../utils/notifyEnd.sh

startChkID=1
finalChkID=5

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
  echo "do nothing"
  exit 1
fi

logDir="${BIN_DIR}/checkCorrectness/log/${testName}"
logFile="${logDir}/groundTruth.log"

./dataLoader.sh 3 ${testName}-o
./dataLoader.sh 4 ${testName}-l
./dataLoader.sh 0 ${inputTopicName} ${inputFilePath}

cd ..

############ Create Baseline Out ############
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

## Get jobID
echo "*** Get jobID ***"
mainJobid=`getRunningJobID`
echo "(mainJobid=${mainJobid})"

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

############ Create Replay Out from chk-(idx) ############
for idx in `seq ${startChkID} ${finalChkID}`
do
  logFile="${logDir}/output_from_chk-${idx}.log"
  # start logger
  echo "*** Start logger ***"
  echo "(startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null)"
  startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null

  # submit job
  echo "*** Submit job ***"
  echo "(./lineage.sh ${JAR_PATH} ${mainPath} ${mainJobid} ${idx} ${testName}-l)"
  ./lineage.sh ${JAR_PATH} ${mainPath} ${mainJobid} ${idx} ${testName}-l

  ## Notify all outputs were provided.
  echo "*** Notify all outputs were provided ***"
  echo "(notifyEnd ${logFile})"
  notifyEnd ${logFile}

  ## Cancel Flink job
  echo "*** Cancel Flink job ***"
  echo "(cancelFlinkJobs)"
  cancelFlinkJobs

  ## Stop logger
  echo "*** Stop logger ***"
  echo "(stopLogger)"
  stopLogger
done

cd ../checkCorrectness/log
python cmpReplayOut.py ${testName} ${startChkID} ${finalChkID} ${parseFlag}
