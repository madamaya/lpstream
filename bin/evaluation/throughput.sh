#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh
source ../utils/logger.sh
source ../utils/cpuMemoryLoadLogger.sh
source ./thUtils/thUtils.sh

granularityTemp=100
queries=(Syn1 Syn2 Syn3 LR Nexmark NYC YSB Nexmark2 NYC2 YSB2)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(-1 10 100)
sleepTime=600
homedir=`pwd`

rm finishedComb.csv ./thLog/parameters.log
touch finishedComb.csv ./thLog/parameters.log

for inputRateIdx in `seq 0 9`
do
  for size in ${sizes[@]}
  do
    for approach in ${approaches[@]}
    do
      for query in ${queries[@]}
      do
        cd ${homedir}
        # Skip invalid cases
        if [[ ${query} == *Syn* ]]; then
          if [ ${size} -eq -1 ]; then
            continue
          fi
        else
          if [ ${size} -ne -1 ]; then
            continue
          fi
        fi

        # Skip unstable cases
        echo "isValid" ${query} ${approach} ${size}
        isValid ${query} ${approach} ${size}
        if [[ $? -ne 0 ]]; then
          continue
        fi

        # Define inputRate
        line=`cat ./thConf/config.csv | grep "${query},${approach},${size},"`
        start_value=`echo ${line} | awk -F, '{print $4}'`
        end_value=`echo ${line} | awk -F, '{print $5}'`
        increment_value=`echo ${line} | awk -F, '{print $6}'`
        tmpinputRate=$((start_value + increment_value * inputRateIdx))
        if [ ${increment_value} -lt 100000 ] && [ ${tmpinputRate} -gt 100000 ]; then
          inputRate=$(((tmpinputRate - 100000) * 10 + 100000))
        else
          inputRate=${tmpinputRate}
        fi

        # Skip some cases
        if [ ${inputRateIdx} -eq 0 ]; then
          continue
        fi
        if [ ${inputRate} -ge ${end_value} ]; then
          continue
        fi

        echo "${query},${approach},${size},${inputRate}" >> ./thLog/parameters.log

        cd ../templates
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
          echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size})"
          ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${size}
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
          echo "(./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${outputTopicName} ${size})"
          ./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} ${query}/${approach} 0 ${outputTopicName} ${size}
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
          ../dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} &
        ## cluster
        else
          ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${inputRate} ${granularity} &
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
        cd ${L3_HOME}/data/output/latency
        echo "*** latency calc ***"
        echo "mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}"
        mkdir -p ${L3_HOME}/data/output/latency/${query}/${approach}
        echo "(readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} false false)" # isLatencyExperiment, isRawMode
        readOutput ${outputTopicName} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${withLineage} false false
        echo "(python calcLatencyV2th2.py ${parallelism} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${inputRate} throughput)"
        python calcLatencyV2th2.py ${parallelism} ${L3_HOME}/data/output/latency/${query}/${approach} ${size} ${inputRate} throughput

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
  cd ${L3_HOME}/data/output/throughput
  python throughputCalc.py "${queries}" "${approaches}" "${sizes}"

  cd ${homedir}
  updateValid "${queries}" "${approaches}" "${sizes}"

  cd ${L3_HOME}/data/output
  mkdir -p thEval
  cp -r latency thEval/latency${inputRateIdx}
  cp -r throughput thEval/throughput${inputRateIdx}
  cp -r cpu-memory thEval/cpu-memory${inputRateIdx}
  ./flesh.sh fleshAll
done

cd ${L3_HOME}/bin/evaluation
mv finishedComb.csv finishedComb.csv.`date -Iseconds`