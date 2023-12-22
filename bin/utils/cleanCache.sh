#!/bin/zsh

source $(dirname $0)/../config.sh

function forceGConTM() {
  tms=(`echo ${flinkTMIP} | sed -e 's/,/ /g'`)
  for tm in ${tms[@]}
  do
    if [ ${tm} = "localhost" ]; then
      jcmd | grep TaskManager | awk '{print $1}' | xargs -I {} jcmd {} GC.run
    else
      ssh ${tm} "jcmd | grep TaskManager | awk '{print \$1}' | xargs -I {} jcmd {} GC.run"
    fi
  done
}

function cleanCache() {
  forceGConTM
  if [ `uname` = "Linux" ]; then
    # FlinkJM
    echo "ssh ${flinkIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
    ssh ${flinkIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh
    # FlinkTM
    TMs=(`echo ${flinkTMIP} | sed -e "s/,/ /g"`)
    for TM in ${TMs[@]}
    do
      echo "ssh ${TM} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
      ssh ${TM} /bin/zsh ${L3_HOME}/run_drop_caches.sh
    done
    # IngestNode
    echo "ssh ${ingestNode} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
    ssh ${ingestNode} /bin/zsh ${L3_HOME}/run_drop_caches.sh
    # Redis
    echo "ssh ${redisIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
    ssh ${redisIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh
    # Kafka zookeeper
    echo "ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
    ssh ${zookeeperIP} /bin/zsh ${L3_HOME}/run_drop_caches.sh
    # Kafka brokers
    brokers=(`echo ${bootstrapServers} | sed -e "s/:9092//g" | sed -e "s/,/ /g"`)
    for broker in ${brokers[@]}
    do
      echo "ssh ${broker} /bin/zsh ${L3_HOME}/run_drop_caches.sh"
      ssh ${broker} /bin/zsh ${L3_HOME}/run_drop_caches.sh
   done
  fi
}