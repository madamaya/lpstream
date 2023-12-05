#!/bin/zsh

source $(dirname $0)/../config.sh
source ./flinkJob.sh
source ./kafkaUtils.sh
source ./redisUtils.sh
source ./cleanCache.sh

stopBroker
stopZookeeper
#startZookeeper
#startBroker
#stopRedis
#startRedis
stopFlinkCluster
#startFlinkCluster
#cleanCache