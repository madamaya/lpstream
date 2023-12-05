#!/bin/zsh

source $(dirname $0)/../config.sh

function cleanCache() {
  if [ `uname` = "Linux" ]; then
    # FlinkJM
    echo "ssh ${flinkIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${flinkIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # FlinkTM
    TMs=(`echo ${flinkTMIP} | tr , " "`)
    for TM in ${TMs[@]}
    do
      echo "ssh ${TM} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
      ssh ${TM} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    done
    # IngestNode
    echo "ssh ${ingestNode} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${ingestNode} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Redis
    echo "ssh ${redisIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${redisIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka zookeeper
    echo "ssh ${zookeeperIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
    ssh ${zookeeperIP} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
    # Kafka brokers
    brokers=(`echo ${bootstrapServers} | tr :9092 " " | tr , " "`)
    for broker in ${brokers[@]}
    do
      echo "ssh ${broker} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
      ssh ${broker} /bin/zsh sync; sync; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
   done
  fi
}