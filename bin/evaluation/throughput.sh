#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/cpmanager.sh
source ../utils/flinkJob.sh
source ../utils/kafkaUtils.sh
source ../utils/redisUtils.sh
source ../utils/cleanCache.sh
source ../utils/logger.sh
source ../utils/cpuMemoryLoadLogger.sh
source ./thUtils/thUtils.sh

granularityTemp=100
queries=(Syn1 Syn2 Syn3)
approaches=(baseline genealog l3stream l3streamlin)
sizes=(10 100 400)
sleepTime=180
inputRates=(5000)
homedir=`pwd`
loop=1

echo "" > finishedComb.csv

for inputRate in ${inputRates[@]}
do
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

        echo "isValid" ${query} ${approach} ${size}
        isValid ${query} ${approach} ${size}
        if [[ $? -ne 0 ]]; then
          continue
        fi

        cd ../templates
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

        echo "(forceGConTM)"
        forceGConTM
        echo "(sleep 10)"
        sleep 10

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
          echo "(./original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${size})"
          ./original.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${size}
        elif [ ${approach} = "genealog" ]; then
          mainPath="com.madamaya.l3stream.workflows.${(L)query}.GL${query}"
          # Run
          echo "*** Run ***"
          echo "(./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${aggregateStrategy} ${size})"
          ./genealog.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${aggregateStrategy} ${size}
        elif [ ${approach} = "l3stream" ]; then
          mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
          # Run
          echo "*** Run ***"
          echo "(./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${size})"
          ./nonlineage.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${size}
        elif [ ${approach} = "l3streamlin" ]; then
          mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
          # Run
          echo "*** Run ***"
          echo "(./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy} ${size})"
          ./lineageNoReplay.sh ${JAR_PATH} ${mainPath} ${parallelism} metrics1/${query}/${approach} 0 ${outputTopicName} ${aggregateStrategy} ${size}
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
        startCpuMemoryLogger ${L3_HOME}/data/output/cpu-memory/${query}/${approach} ${loop}_${size}.log &

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

        # Start latency calc
        echo "*** Latency calculation on Flink ***"
        ## Start flink program
        echo "(./latencyCalc.sh ${JAR_PATH} ${parallelism} ${query} ${L3_HOME}/data/output/latency/metrics1/${query}/${approach}/${loop}_${size}.log)"
        ./latencyCalc.sh ${JAR_PATH} ${parallelism} ${query} ${L3_HOME}/data/output/latency/metrics1/${query}/${approach}/${loop}_${size}.log
        echo "(sleep 30)"
        sleep 30
        ## notify program end
        for idx in `seq 0 ${parallelism}`
        do
          echo "(notifyEnd ${L3_HOME}/data/output/latency/metrics1/${query}/${approach}/${loop}_${size}_${idx}.log)"
          notifyEnd ${L3_HOME}/data/output/latency/metrics1/${query}/${approach}/${loop}_${size}_${idx}.log
        done
        echo "(cancelFlinkJobs)"
        cancelFlinkJobs

        # Read output
        echo "*** Read all outputs ***"
        echo "(readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics1/${query}/${approach} ${loop}_${size}.log ${outputTopicName})"
        readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics1/${query}/${approach} ${loop}_${size}.log ${outputTopicName}

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
  #cd ${L3_HOME}/data/output/latency/metrics1
  #python metrics1.py latency True True True "${queries}" "${approaches}" "${sizes}"
  cd ${L3_HOME}/data/output/throughput/metrics1
  python metrics1.py "${queries}" "${approaches}" "${sizes}"

  cd ${homedir}
  updateValid "${queries}" "${approaches}" "${sizes}" "${inputRate}"

  cd ${L3_HOME}/data/output
  ./getResult.sh
  mkdir -p thEval
  cp -r latency thEval/latency${inputRate}
  cp -r throughput thEval/throughput${inputRate}
  cp -r cpu-memory thEval/cpu-memory${inputRate}
  mv results thEval/results${inputRate}
  ./flesh.sh fleshAll
done
