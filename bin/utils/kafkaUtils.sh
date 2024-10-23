#!/bin/zsh

source $(dirname $0)/../config.sh

function stopZookeeper() {
  if [ ${zookeeperIP} = "localhost" ]; then
    echo "${KAFKA_HOME}/bin/zookeeper-server-stop.sh"
    ${KAFKA_HOME}/bin/zookeeper-server-stop.sh
  else
    echo "ssh ${zookeeperIP} /bin/zsh ${KAFKA_HOME}/bin/zookeeper-server-stop.sh"
    ssh ${zookeeperIP} /bin/zsh ${KAFKA_HOME}/bin/zookeeper-server-stop.sh
  fi
}

function startZookeeper() {
  if [ ${zookeeperIP} = "localhost" ]; then
    while true
    do
      echo "${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties"
      ${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
      sleep 10
      checkZookeeperProcess=`ps aux | grep kafka | wc -l`
      if [ ${checkZookeeperProcess} -eq 2 ]; then
        echo "Zookeeper on ${zookeeperIP} [start] -> OK."
        break
      fi
      echo "Zookeeper on ${zookeeperIP} [start] -> Failed (${checkZookeeperProcess})."
      stopZookeeper
    done
  else
    while true
    do
      echo "ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/bin/utils/zookeeper-start.sh"
      ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/bin/utils/startKafkaCluster/zookeeper-start.sh
      sleep 10
      checkZookeeperProcess=`ssh ${zookeeperIP} ps aux | grep kafka | wc -l`
      if [ ${checkZookeeperProcess} -eq 1 ]; then
        echo "Zookeeper on ${zookeeperIP} [start] -> OK."
        break
      fi
      echo "Zookeeper on ${zookeeperIP} [start] -> Failed."
      stopZookeeper
    done
  fi
}

function stopBroker() {
  if [ ${bootstrapServers} = "localhost:9092" ]; then
    echo "${KAFKA_HOME}/bin/kafka-server-stop.sh"
    ${KAFKA_HOME}/bin/kafka-server-stop.sh
  else
    brokers=(`echo ${bootstrapServers} | sed -e "s/:9092//g" | sed -e "s/,/ /g"`)
    for broker in ${brokers[@]}
    do
      echo "ssh ${broker} /bin/zsh ${KAFKA_HOME}/bin/kafka-server-stop.sh"
      ssh ${broker} /bin/zsh ${KAFKA_HOME}/bin/kafka-server-stop.sh
    done
  fi
}

function startBroker() {
  if [ ${bootstrapServers} = "localhost:9092" ]; then
    while true
    do
      echo "${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties"
      ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
      sleep 10
      checkZookeeperProcess=`ps aux | grep kafka | wc -l`
      if [ ${checkZookeeperProcess} -eq 3 ]; then
        echo "Broker on ${zookeeperIP} [start] -> OK."
        break
      fi
      echo "Broker on ${zookeeperIP} [start] -> Failed (${checkZookeeperProcess})."
      stopBroker
    done
  else
      brokers=(`echo ${bootstrapServers} | sed -e "s/:9092//g" | sed -e "s/,/ /g"`)
      for broker in ${brokers[@]}
      do
        while true
        do
          echo "ssh ${broker} /bin/zsh ${L3_HOME}/bin/utils/broker-start.sh"
          ssh ${broker} /bin/zsh ${L3_HOME}/bin/utils/startKafkaCluster/broker-start.sh
          sleep 10
          brokerProcess=`ssh ${broker} ps aux | grep kafka | wc -l`
          if [ ${brokerProcess} -eq 1 ]; then
            echo "Broker on ${broker} [start] -> OK"
            break
          fi
          echo "Broker on ${broker} [start] -> Failed"
          stopBroker
        done
      done
  fi
}
