#!/bin/zsh

source ../config.sh

if [ $1 -eq 1 ]; then
  source ../workflowConf/configLR.sh
elif [ $1 -eq 2 ]; then
  source ../workflowConf/configNexmark.sh
elif [ $1 -eq 3 ]; then
  source ../workflowConf/configNexmark2.sh
elif [ $1 -eq 4 ]; then
  source ../workflowConf/configNYC.sh
elif [ $1 -eq 5 ]; then
  source ../workflowConf/configNYC2.sh
elif [ $1 -eq 6 ]; then
  source ../workflowConf/configYSB.sh
elif [ $1 -eq 7 ]; then
  source ../workflowConf/configYSB2.sh
elif [ $1 -eq 8 ]; then
  source ../workflowConf/configSyn1.sh
elif [ $1 -eq 9 ]; then
  source ../workflowConf/configSyn2.sh
else
  source ../workflowConf/configSyn3.sh
fi

size=$2
inputRate=50000
granularityTemp=100
sleepTime=720

source ../utils/logger.sh
source ../utils/notifyEnd.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh

# Call Lineage Manager (normal mode)
# This driver emulates the call from "Program Converter" to "Lineage Manager".
# Therefore, "ChkDir, CpMServerIP, CpMServerPort, RedisIP, and RedisPort" have been already configured.
# Note that, to automatically select lineage target tuple, all output are stored in "${L3_HOME}/date/log/${testName}/xxx.log"

# Define log file
echo "*** Define log file ***"
echo "logDir=\"${L3_HOME}/data/log/${(L)query}\""
logDir="${L3_HOME}/data/log/${(L)query}"

if [ ! -d ${logDir} ]; then
  mkdir ${logDir}
fi

## Initialize redis
echo "*** Initialize redis ***"
if [ ${redisIP} = "localhost" ]; then
  echo "redis-cli -h ${redisIP} flushdb"
  redis-cli -h ${redisIP} flushdb
else
  echo "ssh ${redisIP} redis-cli -h ${redisIP} flushdb"
  ssh ${redisIP} redis-cli -h ${redisIP} flushdb
fi

## submit Flink job
cd ${BIN_DIR}/templates
echo "*** Submit Flink job ***"
echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/l3stream 1 ${size})"
./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/l3stream 1 ${size}

while true
do
  running=`getRunningJobID`
  echo "running =" ${running}
  if [ ${running} != "-1" ]; then
    echo "(sleep 5)"
    sleep 5
    echo "break"
    break
  fi
done

# Start data ingestion
echo "Start data ingestion"
if [[ ${query} == *Syn* ]]; then
  filePath="${L3_HOME}/data/input/data/${(L)query}.${size}.csv"
elif [[ ${query} == *LR* ]] || [[ ${query} == *NYC* ]]; then
  filePath="${L3_HOME}/data/input/data/${(L)query}.csv"
else
  filePath="${L3_HOME}/data/input/data/${(L)query}.json"
fi
qName=${query}
topic=${query}-i
granularity=${granularityTemp}
## localhost
if [ ${ingestNode} = "localhost" ]; then
  ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} &
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} &
fi

cd ${BIN_DIR}
echo "*** Get jobid ***"
echo "(jobid=\`getRunningJobID\`)"
jobid=`getRunningJobID`

# Sleep
echo "*** Sleep predefined time (${sleepTime} [s]) ***"
echo "(sleep ${sleepTime})"
sleep ${sleepTime}

## Cancel Flink job
echo "*** Cancel Flink job ***"
echo "(cancelFlinkJobs)"
cancelFlinkJobs

# Stop data ingestion
## localhost
echo "Stop data ingestion"
if [ ${ingestNode} = "localhost" ]; then
  ./dataingest/stopIngestion.sh
## cluster
else
  ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
fi

## Read all data
echo "(readOutput ${outputTopicName} ${logDir} ${size} false false true)" # There is no mean "false false" because these arguments are ignored.
readOutput ${outputTopicName} ${logDir} ${size} false false true

# Call Lineage Manager (lineage mode)
# This driver emulates the call from "User, seeing flink output" to "Lineage Manager".

## Decide and write target outputs for lineage randomly in a log file
echo "*** Decide target output for lineage randomly ***"
echo "(java -cp ${JAR_PATH} com.madamaya.l3stream.utils.Sampling ${logDir} ${size} ${parallelism} ${numOfSamples})"
java -cp ${JAR_PATH} com.madamaya.l3stream.utils.Sampling ${logDir} ${size} ${parallelism} ${numOfSamples}

## Make lineage directory
echo "(mkdir -p ${L3_HOME}/data/output/lineage/${query})"
mkdir -p ${L3_HOME}/data/output/lineage/${query}
echo "(mkdir -p ${L3_HOME}/data/lineage/${query})"
mkdir -p ${L3_HOME}/data/lineage/${query}

cd ${BIN_DIR}/getLineage

## Read target outputs
fileSampledPath="${logDir}/${size}_sampled.csv"
while read LINE
do
  # Stop cluster (Flink, Kafka, Redis)
  echo "(stopBroker)"
  stopBroker < /dev/null
  echo "(stopZookeeper)"
  stopZookeeper < /dev/null
  echo "(stopRedis)"
  stopRedis < /dev/null
  echo "(stopFlinkCluster)"
  stopFlinkCluster < /dev/null

  echo "(sleep 10)"
  sleep 10

  # Remove cache
  echo "(cleanCache)"
  cleanCache < /dev/null

  echo "(sleep 10)"
  sleep 10

  # Start cluster (Flink, Kafka, Redis)
  echo "(startZookeeper)"
  startZookeeper < /dev/null
  echo "(startBroker)"
  startBroker < /dev/null
  echo "(startRedis)"
  startRedis < /dev/null
  echo "(startFlinkCluster)"
  startFlinkCluster < /dev/null

  echo "(sleep 10)"
  sleep 10

  # Remove cache
  echo "(cleanCache)"
  cleanCache < /dev/null
  echo "(sleep 30)"
  sleep 30

  echo "(forceGConTM)"
  forceGConTM < /dev/null
  echo "(sleep 10)"
  sleep 10

  echo "This loop: " ${LINE}
  # GNU awk
  outputValue=`echo ${LINE} | awk 'match($0, /^[0-9]+,[0-9]+,(.*),[0-9]+,[0-9]+$/, ret) {print ret[1]}'`
  outputTs=`echo ${LINE} | awk 'match($0, /^[0-9]+,[0-9]+,.*,([0-9]+),[0-9]+$/, ret) {print ret[1]}'`

  ## Start time
  experimentID=`date "+%s"`

  ## Start Lineage Manager with normal mode
  ./lineageManager.sh ${JAR_PATH} ${mainPath} ${jobid} ${outputTs} ${outputValue} ${maxWindowSize} ${lineageTopicName} ${query} ${size} ${experimentID}

  # Delete kafka topic
  echo "*** Delete kafka topic ***"
  echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers}
  echo "(sleep 30)"
  sleep 30

  # Create kafka topic
  echo "*** Create kafka topic ***"
  echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  echo "(sleep 10)"
  sleep 10
done < ${fileSampledPath}

# Delete kafka topic
echo "*** Delete kafka topic ***"
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers}
echo "(sleep 30)"
sleep 30

# Create kafka topic
echo "*** Create kafka topic ***"
echo "${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(sleep 10)"
sleep 10