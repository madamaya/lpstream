#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh
source ../utils/logger.sh
source ../utils/cpuMemoryLoadLogger.sh

original_throughput=${1}
granularityTemp=100
queries=(Syn1 Syn2 Syn3 LR NYC Nexmark YSB NYC2 Nexmark2 YSB2)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(-1 10 100 400)
sleepTime=600

cd ../templates

for size in ${sizes[@]}
do
  for approach in ${approaches[@]}
  do
    for query in ${queries[@]}
    do
      if [[ ${query} == *Syn* ]]; then
        if [ ${size} -eq -1 ]; then
          continue
        fi
      else
        if [ ${size} -ne -1 ]; then
          continue
        fi
      fi

      # In some cases, input rate make smaller than given one to keep flink stable.
      # This lines can be defined after throughput evaluation.
      if { [[ ${query} == "Syn2" ]] && [[ ${size} == 400 ]] } || { [[ ${query} == "Syn3" ]] && [[ ${size} == 400 ]] }; then
        throughput=10000
      else
        throughput=${original_throughput}
      fi

      # Stop cluster (Flink, Kafka, Redis)
      echo "(stopBroker)"
      stopBroker
      echo "(stopZookeeper)"
      stopZookeeper
      echo "ssh ${redisIP} redis-cli -h ${redisIP} flushdb"
      ssh ${redisIP} redis-cli -h ${redisIP} flushdb
      echo "(stopRedis)"
      stopRedis
      echo "(stopFlinkCluster)"
      stopFlinkCluster

      echo "(sleep 10)"
      sleep 10

      # Remove cache
      echo "(cleanCache)"
      cleanCache

      echo "(sleep 10)"
      sleep 10

      # Start cluster (Flink, Kafka, Redis)
      echo "(startZookeeper)"
      startZookeeper
      echo "(startBroker)"
      startBroker
      echo "(startRedis)"
      startRedis
      echo "ssh ${redisIP} redis-cli -h ${redisIP} flushdb"
      ssh ${redisIP} redis-cli -h ${redisIP} flushdb
      echo "(startFlinkCluster)"
      startFlinkCluster

      echo "(sleep 10)"
      sleep 10

      # Remove cache
      echo "(cleanCache)"
      cleanCache
      echo "(sleep 30)"
      sleep 30

      echo "(forceGConTM)"
      forceGConTM
      echo "(sleep 10)"
      sleep 10

      echo "*** Start evaluation (query = ${query}, approach = ${approach}) ***"

      # restartTMifNeeded
      echo "*** restartTMifNeeded ***"
      restartTMifNeeded

      echo "*** Read config ***"
      # source ./config/${approach}_${query}.sh
      outputTopicName="${query}-o"

      # Start query
      if [ ${approach} = "baseline" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.${query}"
        # Run
        echo "*** Run ***"
        echo "(./original.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size})"
        ./original.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size}
      elif [ ${approach} = "genealog" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.GL${query}"
        # Run
        echo "*** Run ***"
        echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${aggregateStrategy} ${size})"
        ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${aggregateStrategy} ${size}
      elif [ ${approach} = "l3stream" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size})"
        ./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size}
      elif [ ${approach} = "l3streamlin" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy} ${size})"
        ./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy} ${size}
      fi

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
        ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
      ## cluster
      else
        ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
      fi

      # Start CPU/Memory logger
      startCpuMemoryLogger ${L3_HOME}/data/output/cpu-memory/${query}/${approach} ${size}.log &

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

      # Stop data ingestion
      ## localhost
      echo "Stop data ingestion"
      if [ ${ingestNode} = "localhost" ]; then
        ../dataingest/stopIngestion.sh
      ## cluster
      else
        ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh
      fi

      # Initialize 'withLineage' flag
      if [[ ${approach} == "genealog" ]] || [[ ${approach} == "l3streamlin" ]]; then
        withLineage="true"
      else
        withLineage="false"
      fi

      # Latency calculation
      cd ${L3_HOME}/data/output
      echo "*** latency calc ***"
      echo "mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}"
      mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}
      echo "(readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} true false)" # isLatencyExperiment, isRawMode
      readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} true false
      echo "(python calcLatencyV2.py ${parallelism} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} latency)"
      python calcLatencyV2.py ${parallelism} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} latency

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
python cpu-memory.py "${queries}" "${approaches}" "${sizes}"
cd ${L3_HOME}/data/output/latency
python resultsGen.py "${queries}" "${approaches}" "${sizes}"
cd ${L3_HOME}/data/output/throughput/metrics1
python throughputCalc.py "${queries}" "${approaches}" "${sizes}"

cd ${L3_HOME}/data/output
mkdir -p latEval
cp -r latency latEval/latency${throughput}
cp -r throughput latEval/throughput${throughput}
cp -r cpu-memory latEval/cpu-memory${throughput}
./flesh.sh fleshAll
