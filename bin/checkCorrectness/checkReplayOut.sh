#!/bin/zsh

source ../config.sh
source ../utils/cpmanager.sh
source ../utils/logger.sh
source ../utils/flinkJob.sh
source ../utils/notifyEnd.sh

startChkID=3
finalChkID=5
sleepTime=60
granularityTemp=10
throughput=10000
datanum=300000

redis-cli -h ${redisIP} FLUSHDB

if [ $# -ne 1 ]; then
  echo "Illegal Args"
  exit 1
fi

if [ $1 -eq 1 ]; then
  echo "****************** LR ******************"
  source ./config/configLR.sh
elif [ $1 -eq 2 ]; then
  echo "****************** LR2 ******************"
  source ./config/configLR2.sh
elif [ $1 -eq 3 ]; then
  echo "****************** Nexmark ******************"
  source ./config/configNexmark.sh
elif [ $1 -eq 4 ]; then
  echo "****************** NYC ******************"
  source ./config/configNYC.sh
elif [ $1 -eq 5 ]; then
  echo "****************** YSB ******************"
  source ./config/configYSB.sh
elif [ $1 -eq 6 ]; then
  echo "****************** Syn1 ******************"
  source ./config/configSyn1.sh
elif [ $1 -eq 7 ]; then
  echo "****************** Syn2 ******************"
  source ./config/configSyn2.sh
elif [ $1 -eq 8 ]; then
  echo "****************** Syn3 ******************"
  source ./config/configSyn3.sh
fi

logDir="${BIN_DIR}/checkCorrectness/log/${testName}"
logFile="${logDir}/nonlineageMode.log"

if [ ! -d ${logDir} ]; then
  mkdir ${logDir}
fi

############ Create NonlineageMode Out ############
./dataLoader.sh 3 ${testName}-i
./dataLoader.sh 3 ${testName}-o
./dataLoader.sh 3 ${testName}-l

# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-o > /dev/null

# submit job
cd ../templates
echo "sumbit job"
echo "(./nonlineageCpAssigner.sh ${JAR_PATH} ${mainL3Path} ${parallelism})"
./nonlineageCpAssigner.sh ${JAR_PATH} ${mainL3Path} ${parallelism}

# Start data ingestion
sleep 10
echo "Start data ingestion"
if [ ${testName} = "LR" ] || [ ${testName} = "LR2" ] || [ ${testName} = "NYC" ] || [ ${testName} = "Syn1" ] || [ ${testName} = "Syn2" ] || [ ${testName} = "Syn3" ]; then
  filePath="${L3_HOME}/data/input/data/${(L)testName}.csv"
else
  filePath="${L3_HOME}/data/input/data/${(L)testName}.json"
fi
qName=${testName}
topic=${testName}-i
granularity=${granularityTemp}
## localhost
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
fi

echo "*** Sleep (${sleepTime}) ***"
sleep ${sleepTime}

# Stop data ingestion
## localhost
echo "Stop data ingestion"
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/stopIngestion.sh
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
fi

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

## Stop kafka logger
echo "*** Stop kafka logger ***"
echo "(stopLogger)"
stopLogger

############ Create Replay Out from chk-(idx) ############
for idx in `seq ${startChkID} ${finalChkID}`
do
  logFile="${logDir}/lineageMode${idx}.log"
  # start logger
  echo "*** Start logger ***"
  echo "(startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null)"
  startKafkaLogger ${logDir} ${logFile} ${testName}-l > /dev/null

  # submit job
  echo "*** Submit job ***"
  echo "(./lineageReplay.sh ${JAR_PATH} ${mainL3Path} ${parallelism} ${mainJobid} ${idx} ${testName}-l 2 ${aggregateStrategy})"
  ./lineageReplay.sh ${JAR_PATH} ${mainL3Path} ${parallelism} ${mainJobid} ${idx} ${testName}-l 2 ${aggregateStrategy}

  sleep 60

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

############ Create Baseline Out ############
cd ../checkCorrectness
./dataLoader.sh 3 ${testName}-i
./dataLoader.sh 3 ${testName}-o
./dataLoader.sh 3 ${testName}-l

logFile="${logDir}/baseline.log"
# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-o > /dev/null

cd ../templates
# submit job
echo "sumbit job"
./original.sh ${JAR_PATH} ${mainPath} ${parallelism}

# Start data ingestion
sleep 10
echo "Start data ingestion"
if [ ${testName} = "LR" ] || [ ${testName} = "LR2" ] || [ ${testName} = "NYC" ] || [ ${testName} = "Syn1" ] || [ ${testName} = "Syn2" ] || [ ${testName} = "Syn3" ]; then
  filePath="${L3_HOME}/data/input/data/${(L)testName}.csv"
else
  filePath="${L3_HOME}/data/input/data/${(L)testName}.json"
fi
qName=${testName}
topic=${testName}-i
granularity=${granularityTemp}
## localhost
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
fi

echo "*** Sleep (${sleepTime}) ***"
sleep ${sleepTime}

# Stop data ingestion
## localhost
echo "Stop data ingestion"
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/stopIngestion.sh
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
fi

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

## Stop kafka logger
echo "*** Stop kafka logger ***"
echo "(stopLogger)"
stopLogger

############ Create Genealog Out ############
cd ../checkCorrectness
./dataLoader.sh 3 ${testName}-i
./dataLoader.sh 3 ${testName}-o
./dataLoader.sh 3 ${testName}-l

logFile="${logDir}/genealog.log"
# start logger
echo "start logger"
startKafkaLogger ${logDir} ${logFile} ${testName}-o > /dev/null

cd ../templates
# submit job
echo "sumbit job"
./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${aggregateStrategy}

# Start data ingestion
sleep 10
echo "Start data ingestion"
if [ ${testName} = "LR" ] || [ ${testName} = "LR2" ] || [ ${testName} = "NYC" ] || [ ${testName} = "Syn1" ] || [ ${testName} = "Syn2" ] || [ ${testName} = "Syn3" ]; then
  filePath="${L3_HOME}/data/input/data/${(L)testName}.csv"
else
  filePath="${L3_HOME}/data/input/data/${(L)testName}.json"
fi
qName=${testName}
topic=${testName}-i
granularity=${granularityTemp}
## localhost
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${datanum} &
fi

echo "*** Sleep (${sleepTime}) ***"
sleep ${sleepTime}

# Stop data ingestion
## localhost
echo "Stop data ingestion"
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/stopIngestion.sh
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
fi

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

## Stop kafka logger
echo "*** Stop kafka logger ***"
echo "(stopLogger)"
stopLogger

cd ../checkCorrectness/log
python cmpReplayOut.py ${testName} ${startChkID} ${finalChkID} ${parseFlag}
