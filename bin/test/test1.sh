#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh
source ../utils/logger.sh
source ../utils/cpuMemoryLoadLogger.sh

throughput=10000
granularityTemp=100
queries=(Syn1 Syn2 Syn3 LR Nexmark NYC YSB Nexmark2 NYC2 YSB2)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(-1 10 100 400)
data_num=600000
sleepTime=90
homedir=`pwd`

for size in ${sizes[@]}
do
  for approach in ${approaches[@]}
  do
    for query in ${queries[@]}
    do
      cd ${homedir}

      if [[ ${query} == *Syn* ]]; then
        if [ ${size} -eq -1 ]; then
          continue
        fi
      else
        if [ ${size} -ne -1 ]; then
          continue
        fi
      fi

      cd ../templates
      if [ ${redisIP} = "localhost" ]; then
        echo "redis-cli -h ${redisIP} flushdb"
        redis-cli -h ${redisIP} flushdb
      else
        echo "ssh ${redisIP} redis-cli -h ${redisIP} flushdb"
        ssh ${redisIP} redis-cli -h ${redisIP} flushdb
      fi

      echo "*** Start test1 (query = ${query}, approach = ${approach}, size = ${size}) ***"

      # restartTMifNeeded
      echo "*** restartTMifNeeded ***"
      restartTMifNeeded

      echo "*** Read config ***"
      outputTopicName="${query}-o"

      # Start query
      if [ ${approach} = "baseline" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.${query}"
        # Run
        echo "*** Run ***"
        echo "(./original.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 1 ${size})"
        ./original.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 1 ${size}
      elif [ ${approach} = "genealog" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.GL${query}"
        # Run
        echo "*** Run ***"
        echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size})"
        ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size}
      elif [ ${approach} = "l3stream" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size})"
        ./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${size}
      elif [ ${approach} = "l3streamlin" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${outputTopicName} ${size})"
        ./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 100 ${outputTopicName} ${size}
      fi

      echo "(sleep 15)"
      sleep 15

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
        ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${data_num} &
      ## cluster
      else
        ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} ${data_num} &
      fi

      # Start CPU/Memory logger
      startCpuMemoryLogger ${L3_HOME}/data/output/cpu-memory/${query}/${approach} ${size}.log &

      # Sleep
      echo "*** Sleep predefined time (${sleepTime} [s]) ***"
      echo "(sleep ${sleepTime})"
      sleep ${sleepTime}

      # Stop CPU/Memory logger
      stopCpuMemoryLogger

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

      cd ${L3_HOME}/data/output/latency
      echo "*** latency calc ***"
      echo "mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}"
      mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}
      echo "(readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} true true)" # isLatencyExperiment, isRawMode
      readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} true true

      # Stop query
      echo "*** Cancel running flink job ***"
      echo "(cancelFlinkJobs)"
      cancelFlinkJobs

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

cd ${L3_HOME}/bin/test/scripts
echo "(python test1.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/latency)"
python test1.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/latency
echo "(python test1_outputSet.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/latency)"
python test1_outputSet.py "${queries}" "${approaches}" "${sizes}" ${L3_HOME}/data/output/latency

cd ${L3_HOME}/data/output
mkdir -p test1
cp -r latency test1/latency
cp -r throughput test1/throughput
cp -r cpu-memory test1/cpu-memory
./flesh.sh fleshAll
