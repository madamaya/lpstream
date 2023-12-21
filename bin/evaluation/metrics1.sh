#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/cpmanager.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh
source ../utils/logger.sh
source ../utils/cpuMemoryLoadLogger.sh

numOfLoop=3
throughput=${1}
granularityTemp=10
#queries=(LR2 Nexmark NYC Nexmark2 YSB)
queries=(LR2 NYC Nexmark2 YSB)
#queries=(Nexmark NYC Nexmark2 YSB)
approaches=(baseline genealog l3stream l3streamlin)
#approaches=(baseline)
sleepTime=180

cd ../templates

for loop in `seq 1 ${numOfLoop}`
do
  for approach in ${approaches[@]}
  do
    for query in ${queries[@]}
    do
      # Stop cluster (Flink, Kafka, Redis)
      echo "(stopBroker)"
      stopBroker
      echo "(stopZookeeper)"
      stopZookeeper
      echo "(stopRedis)"
      stopRedis
      echo "(stopFlinkCluster)"
      stopFlinkCluster

      echo "(sleep 30)"
      sleep 30

      # Remove cache
      echo "(cleanCache)"
      cleanCache

      echo "(sleep 30)"
      sleep 30

      # Start cluster (Flink, Kafka, Redis)
      echo "(startZookeeper)"
      startZookeeper
      echo "(startBroker)"
      startBroker
      echo "(startRedis)"
      startRedis
      echo "(startFlinkCluster)"
      startFlinkCluster

      echo "(sleep 30)"
      sleep 30

      # Remove cache
      echo "(cleanCache)"
      cleanCache

      echo "(sleep 120)"
      sleep 120

      if [ ${query} = "Nexmark" ]; then
        #sleepTime=600
        sleepTime=300
      elif [ ${query} = "Nexmark2" ]; then
        #sleepTime=600
        sleepTime=300
      else
        #sleepTime=180
        sleepTime=300
      fi

      echo "*** Start evaluation (query = ${query}, approach = ${approach}, loop = ${loop}) ***"

      # restartTMifNeeded
      echo "*** restartTMifNeeded ***"
      restartTMifNeeded

      echo "*** Read config ***"
      # source ./config/${approach}_${query}.sh
      outputTopicName="${query}-o"

      # Decide strategy
      aggregateStrategy="unsortedPtr"

      # Start query
      if [ ${approach} = "baseline" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.${query}"
        # Run
        echo "*** Run ***"
        echo "(./original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0)"
        ./original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0
      elif [ ${approach} = "genealog" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.GL${query}"
        # Run
        echo "*** Run ***"
        echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${aggregateStrategy})"
        ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${aggregateStrategy}
      elif [ ${approach} = "l3stream" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0)"
        ./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0
      elif [ ${approach} = "l3streamlin" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy})"
        ./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy}
      fi

      # Start data ingestion
      echo "Start data ingestion"
      if [ ${query} = "LR" ] || [ ${query} = "LR2" ] || [ ${query} = "NYC" ]; then
        filePath="${L3_HOME}/data/input/data/${(L)query}.csv"
      else
        filePath="${L3_HOME}/data/input/data/${(L)query}.json"
      fi
      qName=${query}
      topic=${query}-i
      granularity=${granularityTemp}
      ## localhost
      if [ ${ingestNode} = "localhost" ]; then
        ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
      ## cluster
      else
        ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
      fi

      # Start CPU/Memory logger
      startCpuMemoryLogger ${L3_HOME}/data/output/cpu-memory/${query}/${approach} ${loop}.log &

      # Sleep
      echo "*** Sleep predefined time (${sleepTime} [s]) ***"
      echo "(sleep ${sleepTime})"
      sleep ${sleepTime}

      # Stop CPU/Memory logger
      stopCpuMemoryLogger

      # Stop query
      echo "*** Cancel running flink job ***"
      echo "(cancelFlinkJobs)"
      cancelFlinkJobs

      #if [ ${approach} = "l3stream" ]; then
        # Stop CpMServer
        #echo "*** Stop CpMServer ***"
        #echo "(stopCpMServer)"
        #stopCpMServer
      #fi

      # Stop data ingestion
      ## localhost
      echo "Stop data ingestion"
      if [ ${ingestNode} = "localhost" ]; then
        ../dataingest/stopIngestion.sh
      ## cluster
      else
        ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
      fi

      # Read output
      echo "*** Read all outputs ***"
      echo "(readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics1/${query}/${approach} ${loop}.log ${outputTopicName})"
      readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics1/${query}/${approach} ${loop}.log ${outputTopicName}

      # Delete kafka topic
      echo "*** Delete kafka topic ***"
      echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers})"
      ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${outputTopicName} --bootstrap-server ${bootstrapServers}
      echo "(${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers})"
      ${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic ${query}-i --bootstrap-server ${bootstrapServers}
      echo "(sleep 30)"
      sleep 30

      # Create kafka topic
      echo "*** Create kafka topic ***"
      echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism})"
      ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${outputTopicName} --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
      echo "${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}"
      ${KAFKA_HOME}/bin/kafka-topics.sh --create --topic ${query}-i --bootstrap-server ${bootstrapServers} --partitions ${parallelism}
      echo "(sleep 10)"
      sleep 10
    done
  done
done

cd ${L3_HOME}/data/output/cpu-memory
python cpu-memory.py
cd ${L3_HOME}/data/output/latency/metrics1
python metrics1.py
cd ${L3_HOME}/data/output/throughput/metrics1
python metrics1.py

cd ${L3_HOME}/data/output
./getResult.sh
cp -r latency latency${throughput}
cp -r throughput throughput${throughput}
cp -r cpu-memory cpu-memory${throughput}
mv results results${throughput}
./flesh.sh fleshAll
