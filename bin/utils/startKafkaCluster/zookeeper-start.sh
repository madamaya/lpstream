#!/bin/zsh

source $(dirname $0)/../../config.sh
${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties