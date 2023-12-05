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
    echo "${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties"
    ${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
  else
    echo "ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/bin/utils/zookeeper-start.sh"
    ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/bin/utils/startKafkaCluster/zookeeper-start.sh
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
    echo "${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties"
    ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
  else
      brokers=(`echo ${bootstrapServers} | sed -e "s/:9092//g" | sed -e "s/,/ /g"`)
      for broker in ${brokers[@]}
      do
        echo "ssh ${broker} /bin/zsh ${L3_HOME}/bin/utils/broker-start.sh"
        ssh ${broker} /bin/zsh ${L3_HOME}/bin/utils/startKafkaCluster/broker-start.sh
      done
  fi
}
