#!/bin/zsh

source $(dirname $0)/../config.sh

function stopRedis() {
  if [ ${redisIP} = "localhost" ]; then
    # Unsupport
  else
    ssh ${redisIP} /bin/zsh redis-cli -h ${redisIP} flushdb
    ssh ${redisIP} /bin/zsh sudo systemctl stop redis-server.service
  fi
}

function startRedis() {
  if [ ${zookeeper} = "localhost" ]; then
    # Unsupport
  else
    ssh ${redisIP} /bin/zsh sudo systemctl start redis-server.service
  fi
}


