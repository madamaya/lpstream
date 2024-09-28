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
inputRate=10000
granularityTemp=100
data_num=600000
sleepTime=90

source ../utils/logger.sh
source ../utils/notifyEnd.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh

for approach in genealog l3stream
do
  # Define log file
  echo "*** Define log file ***"
  echo "logDir=\"${L3_HOME}/data/log/${(L)query}/${approach}\""
  logDir="${L3_HOME}/data/log/${(L)query}/${approach}"

  if [ ! -d ${logDir} ]; then
    mkdir -p ${logDir}
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
  if [ ${approach} = "l3stream" ]; then
    mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
    echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size})"
    ./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size}
  elif [ ${approach} = "genealog" ]; then
    mainPath="com.madamaya.l3stream.workflows.${(L)query}.GL${query}"
    echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size})"
    ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size}
  else
    echo "ERROR"
    exit 1
  fi
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
    ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} ${data_num} &
  ## cluster
  else
    ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} ${data_num} &
  fi

  cd ${BIN_DIR}
  echo "*** Get jobid ***"
  echo "(jobid=\`getRunningJobID\`)"
  jobid=`getRunningJobID`

  # Sleep
  echo "*** Sleep predefined time (${sleepTime} [s]) ***"
  echo "(sleep ${sleepTime})"
  sleep ${sleepTime}

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

  ## Cancel Flink job
  echo "*** Cancel Flink job ***"
  echo "(cancelFlinkJobs)"
  cancelFlinkJobs

  # Delete kafka topic
  echo "*** Delete kafka topic ***"
  echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers}
  echo "(sleep 30)"
  sleep 30

  # Create kafka topic
  echo "*** Create kafka topic ***"
  echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
  ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
  echo "(sleep 10)"
  sleep 10

  if [ ${approach} = "l3stream" ]; then
    cd ${BIN_DIR}/test
    if [ ! -d ${L3_HOME}/bin/test/redis_log ]; then
      mkdir -p ${L3_HOME}/bin/test/redis_log
    fi
    cd ./scripts
    echo "(python make_redis_log.py ${qName} ${size} ${redisIP} ${redisPort} ${parallelism})"
    python make_redis_log.py ${qName} ${size} ${redisIP} ${redisPort} ${parallelism}
  fi
done

cd ${BIN_DIR}/templates
for replay_idx in `seq 1 5`
do
  echo "*** Define log file ***"
  echo "logDir=\"${L3_HOME}/data/log/${(L)query}/l3streamlin/${replay_idx}\""
  logDir="${L3_HOME}/data/log/${(L)query}/l3streamlin/${replay_idx}"

  if [ ! -d ${logDir} ]; then
    mkdir -p ${logDir}
  fi

  ## Replay ordinary workflow from chk-i
  echo "*** Submit job (from chk-${replay_idx}) ***"
  echo "(./lineageReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${jobid} ${replay_idx} ${lineageTopicName} 100)"
  ./lineageReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${jobid} ${replay_idx} ${lineageTopicName} 100

  # Sleep
  echo "*** Sleep predefined time (${sleepTime} [s]) ***"
  echo "(sleep ${sleepTime})"
  sleep ${sleepTime}

  ## Read all data
  echo "(readOutput ${lineageTopicName} ${logDir} ${size} false false true)"
  readOutput ${lineageTopicName} ${logDir} ${size} false false true

  ## Cancel Flink job
  echo "*** Cancel Flink job ***"
  echo "(cancelFlinkJobs)"
  cancelFlinkJobs

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
done

# Delete kafka topic
echo "*** Delete kafka topic ***"
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-o --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-o --bootstrap-server ${bootstrapServers}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers})"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers}
echo "(sleep 30)"
sleep 30

# Create kafka topic
echo "*** Create kafka topic ***"
echo "${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-o --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-o} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${lineageTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
echo "(sleep 10)"
sleep 10