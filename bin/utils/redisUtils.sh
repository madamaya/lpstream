#!/bin/zsh

source $(dirname $0)/../config.sh

function stopRedis() {
  if [ ${redisIP} = "localhost" ]; then
    # Unsupport
  else
    #echo "ssh ${redisIP} redis-cli -h ${redisIP} flushdb"
    #ssh ${redisIP} redis-cli -h ${redisIP} flushdb
    echo "ssh ${redisIP} sudo systemctl stop redis-server.service"
    ssh ${redisIP} sudo systemctl stop redis-server.service
  fi
}

function startRedis() {
  if [ ${redisIP} = "localhost" ]; then
    # Unsupport
  else
    echo "ssh ${redisIP} sudo systemctl start redis-server.service"
    ssh ${redisIP} sudo systemctl start redis-server.service
  fi
}


