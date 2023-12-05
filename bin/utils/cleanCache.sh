#!/bin/zsh

source $(dirname $0)/../config.sh

function cleanCache() {
  if [ `uname` = "Linux" ]; then
    # FlinkJM
    echo "ssh ${flinkIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${flinkIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # FlinkTM
    TMs=(`echo ${flinkTMIP} | sed -e "s/,/ /g"`)
    for TM in ${TMs[@]}
    do
      echo "ssh ${TM} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
      ssh ${TM} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    done
    # IngestNode
    echo "ssh ${ingestNode} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${ingestNode} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Redis
    echo "ssh ${redisIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${redisIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka zookeeper
    echo "ssh ${zookeeperIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${zookeeperIP} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka brokers
    brokers=(`echo ${bootstrapServers} | sed -e "s/:9092//g" | sed -e "s/,/ /g"`)
    for broker in ${brokers[@]}
    do
      echo "ssh ${broker} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
      ssh ${broker} sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
   done
  fi
}