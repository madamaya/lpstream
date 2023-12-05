#!/bin/zsh

source $(dirname $0)/../config.sh

function cleanCache() {
  if [ `uname` = "Linux" ]; then
    # FlinkJM
    ssh ${flinkIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # FlinkTM
    TMs=(`echo ${flinkTMIP} | tr , " "`)
    for TM in ${TMs[@]}
    do
      ssh ${TM} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    done
    # IngestNode
    ssh ${ingestNode} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Redis
    ssh ${redisIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka zookeeper
    ssh ${zookeeperIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka brokers
    brokers=(`echo ${bootstrapServers} | tr :9092 " " | tr , " "`)
    for broker in ${brokers[@]}
    do
      ssh ${broker} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
   done
  fi
}