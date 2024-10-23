#!/bin/zsh

source $(dirname $0)/../../config.sh
${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties