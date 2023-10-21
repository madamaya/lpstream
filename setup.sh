#!/bin/zsh

LRScaleFactor=1
NexmarkTupleNum=1000000
NYCstartYear=2022
NYCendYear=2023
YSBTupleNum=60933230

if [ $# -ne 1 ]; then
  echo "Illegal Arguments."
  exit 1
fi

if [ $1 = "downloads" ]; then
  # download flink
  echo "*** Download flink ***"
  wget https://dlcdn.apache.org/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
  tar xzf flink-1.17.1-bin-scala_2.12.tgz
  mv flink-1.17.1 flink
  cp flink-conf.yaml flink/conf/flink-conf.yaml
  rm flink-1.17.1-bin-scala_2.12.tgz

  # download kafka
  echo "*** Download kafka ***"
  wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz
  tar zxf kafka_2.12-3.5.1.tgz
  mv kafka_2.12-3.5.1 kafka
  rm kafka_2.12-3.5.1.tgz

  echo "*** END ***"
elif [ $1 = "compile" ]; then
  git clone git@github.com:madamaya/l3stream-genealog.git
  python configGenSh2Java.py
  cd l3stream-genealog
  mvn package
  cd ../
  cp l3stream-genealog/target/ananke-1.0-SNAPSHOT.jar ./lib/l3stream-genealog.jar
  mvn package
elif [ $1 = "mainData" ]; then
  # Generate data
  cd ./data/input

  ## Generate data for Linear Road
  cd ./LinearRoad
  ./lrGen.sh ${LRScaleFactor}

  ## Generate data for Nexmark
  cd ../Nexmark
  ./nexmarkGen.sh ${NexmarkTupleNum}

  ## Generate data for NYC
  cd ../NYC
  ./nycGen.sh ${NYCstartYear} ${NYCendYear}

  ## Generate data for YSB
  cd ../YSB
  ./ysbGen.sh ${YSBTupleNum}

  echo "*** END ***"
elif [ $1 = "testData" ]; then
  ## Generate splitted data for result tests
  cd ./data/input

  ### For LinearRoad
  echo "*** Generate test data for LR ***"
  python split.py ./LinearRoad/lr.csv

  ### For Nexmark
  echo "*** Generate test data for Nexmark ***"
  python split.py ./Nexmark/nexmark.json

  ### For NYC
  echo "*** Generate test data for NYC ***"
  python split.py ./NYC/nyc.csv

  ### For YSB
  echo "*** Generate test data for YSB ***"
  python split.py ./YSB/ysb.json

  echo "*** END ***"
elif [ $1 = "setup" ]; then
  ./bin/setupCluster/startCluster.sh
else
  echo "Illegal Arguments"
  exit 1
fi