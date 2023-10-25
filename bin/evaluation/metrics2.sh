#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/cpmanager.sh
source ../utils/flinkJob.sh
source ../utils/logger.sh

numOfLoop=3
sleepTime=30
queries=(LR Nexmark NYC YSB)
approaches=(baseline l3stream)
#approaches=(baseline)

cd ./templates

for loop in `seq 1 ${numOfLoop}`
do
  for approach in ${approaches[@]}
  do
    for query in ${queries[@]}
    do
      echo "*** Start evaluation (query = ${query}, approach = ${approach}, loop = ${loop}) ***"

      echo "*** Read config ***"
      # source ./config/${approach}_${query}.sh
      outputTopicName="${query}-o"

      # Start query
      if [ ${approach} = "baseline" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.${query}"
        # Run
        echo "*** Run ***"
        echo "(./original.sh ${JAR_PATH} ${mainPath} 0 metrics2/${query}/${approach} 1)"
        ./original.sh ${JAR_PATH} ${mainPath} 0 metrics2/${query}/${approach} 1
      elif [ ${approach} = "l3stream" ]; then
        mainPath="com.madamaya.l3stream.workflows.${(L)query}.L3${query}"
        # Run
        echo "*** Run ***"
        echo "(./lineage.sh ${JAR_PATH} ${mainPath} 0 ${outputTopicName} metrics2/${query}/${approach} ${parallelism})"
        ./lineage.sh ${JAR_PATH} ${mainPath} 0 ${outputTopicName} metrics2/${query}/${approach} ${parallelism}
      fi

      # Sleep
      echo "*** Sleep predefined time (${sleepTime} [s]) ***"
      echo "(sleep ${sleepTime})"
      sleep ${sleepTime}

      # Stop query
      echo "*** Cancel running flink job ***"
      echo "(cancelFlinkJobs)"
      cancelFlinkJobs

      if [ ${approach} = "l3stream" ]; then
        # Stop CpMServer
        echo "*** Stop CpMServer ***"
        echo "(stopCpMServer)"
        stopCpMServer
      fi

      # Read output
      echo "*** Read all outputs ***"
      echo "(readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics2/${query}/${approach} ${loop}.log ${outputTopicName})"
      readOutputFromEarliest ${L3_HOME}/data/output/latency/metrics2/${query}/${approach} ${loop}.log ${outputTopicName}

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
    done
  done
done

cd ${L3_HOME}/data/output/latency/metrics2
python metrics2.py
cd ${L3_HOME}/data/output/throughput/metrics2
python metrics2.py
