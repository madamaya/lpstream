#!/bin/zsh

source ./bin/config.sh

# Small size (for local test)
LRScaleFactor=1
NexmarkTupleNum=1000000
NYCstartYear=2022
NYCendYear=2023
YSBTupleNum=60933230

# Large size (for experiments)
#LRScaleFactor=10
#NexmarkTupleNum=275000000
#NYCstartYear=2017
#NYCendYear=2023
#YSBTupleNum=360000000

if [ $# -ne 1 ]; then
  echo "Illegal Arguments."
  exit 1
fi

if [ $1 = "downloads" ]; then
  # download flink
  echo "*** Download flink ***"
  wget https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
  tar xzf flink-1.17.1-bin-scala_2.12.tgz
  mv flink-1.17.1 flink
  cp flink-conf.yaml flink/conf/flink-conf.yaml
  rm flink-1.17.1-bin-scala_2.12.tgz

  # download kafka
  echo "*** Download kafka ***"
  wget https://archive.apache.org/dist/kafka/3.6.0/kafka_2.12-3.6.0.tgz
  tar zxf kafka_2.12-3.6.0.tgz
  mv kafka_2.12-3.6.0 kafka
  rm kafka_2.12-3.6.0.tgz

  echo "*** END ***"
elif [ $1 = "compile" ]; then
  git clone -b experiments git@github.com:madamaya/l3stream-genealog.git
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
  echo "START: ./lrGen.sh ${LRScaleFactor}" >> ../dataGen.log
  date >> ../dataGen.log
  ./lrGen.sh ${LRScaleFactor}
  echo "END: ./lrGen.sh ${LRScaleFactor}" >> ../dataGen.log
  date >> ../dataGen.log

  ## Generate data for Nexmark
  cd ../Nexmark
  echo "START: ./nexmarkGen.sh ${NexmarkTupleNum}" >> ../dataGen.log
  date >> ../dataGen.log
  ./nexmarkGen.sh ${NexmarkTupleNum}
  echo "END: ./nexmarkGen.sh ${NexmarkTupleNum}" >> ../dataGen.log
  date >> ../dataGen.log

  ## Generate data for NYC
  cd ../NYC
  echo "START: ./nycGen.sh ${NYCstartYear} ${NYCendYear}" >> ../dataGen.log
  date >> ../dataGen.log
  ./nycGen.sh ${NYCstartYear} ${NYCendYear}
  echo "END: ./nycGen.sh ${NYCstartYear} ${NYCendYear}" >> ../dataGen.log
  date >> ../dataGen.log

  ## Generate data for YSB
  cd ../YSB
  echo "START: ./ysbGen.sh ${YSBTupleNum}" >> ../dataGen.log
  date >> ../dataGen.log
  ./ysbGen.sh ${YSBTupleNum}
  echo "END: ./ysbGen.sh ${YSBTupleNum}" >> ../dataGen.log
  date >> ../dataGen.log

  ## Generate data for Syn
  cd ../Syn
  echo "START: python dataGen.py ${parallelism}" >> ../dataGen.log
  python dataGen.py ${parallelism}
  echo "END: python dataGen.py ${parallelism}" >> ../dataGen.log
  for i in 10 100 400
  do
    echo "START: python checkDuplicate.py"
    python checkDuplicate.py ${parallelism} ${i}
    echo "END: python checkDuplicate.py"
    echo "START: ./copy.sh ${parallelism} ${i}"
    ./copy.sh ${parallelism} ${i}
    echo "END: ./copy.sh ${parallelism} ${i}"
  done

  ## Distribute benchmark data
  cd ..
  dataList=(lr.csv nyc.csv nyc2.csv nexmark.json nexmark2.json ysb.json ysb2.json)
  for file in ${dataList[@]}
  do
    echo "START: python distribute.py ${file} ${parallelism}" >> ../dataGen.log
    python distribute.py ./data/${file} ${parallelism}
    echo "END: python distribute.py ${file} ${parallelism}" >> ../dataGen.log
  done

  echo "*** END ***"
elif [ $1 = "setup" ]; then
  ./bin/setupCluster/startCluster.sh
else
  echo "Illegal Arguments"
  exit 1
fi