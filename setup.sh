#!/bin/zsh

source ./bin/config.sh
#LRScaleFactor=1
LRScaleFactor=10
#NexmarkTupleNum=1000000
NexmarkTupleNum=275000000
#NYCstartYear=2022
NYCstartYear=2017
NYCendYear=2023
#YSBTupleNum=60933230
YSBTupleNum=360000000

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
  git clone -b kafkaProcessingLatency git@github.com:madamaya/l3stream-genealog.git
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
elif [ $1 = "testData" ]; then
  ## Generate splitted data for result tests
  cd ./data/input

  ### For LinearRoad
  echo "*** Generate test data for LR ***"
  python split.py ./data/lr.csv

  ### For Nexmark
  echo "*** Generate test data for Nexmark ***"
  python split.py ./data/nexmark.json

  ### For NYC
  echo "*** Generate test data for NYC ***"
  python split.py ./data/nyc.csv

  ### For YSB
  echo "*** Generate test data for YSB ***"
  python split.py ./data/ysb.json

  echo "*** END ***"
elif [ $1 = "setup" ]; then
  ./bin/setupCluster/startCluster.sh
elif [ $1 = "test" ]; then
  cd ./bin
  # LR
  echo "*** TEST: LR ***"
  echo "(./LMUserDriver.sh 1)"
  ./LMUserDriver.sh 1 2>& 1 | tee LMUserDriver1.log
  # Nexmark
  echo "*** TEST: Nexmark ***"
  echo "(./LMUserDriver.sh 2)"
  ./LMUserDriver.sh 2 2>& 1 | tee LMUserDriver2.log
  # NYC
  echo "*** TEST: NYC ***"
  echo "(./LMUserDriver.sh 3)"
  ./LMUserDriver.sh 3 2>& 1 | tee LMUserDriver3.log
  # YSB
  echo "*** TEST: YSB ***"
  echo "(./LMUserDriver.sh 4)"
  ./LMUserDriver.sh 4 2>& 1 | tee LMUserDriver4.log

  cd ./checkCorrectness
  echo "*** Start checkCorrectness ***"
  echo "(./auto.sh)"
  ./auto.sh
elif [ $1 = "dataSizeTest" ]; then
  if [ $# -ne 3 ]; then
    echo "Illegal Arguments (dataSizeTest)"
    exit 1
  fi
  cd ./bin/dataSizeTest
  echo "*** dataSizeTest ***"
  echo "(./dataSizeTest.sh $2 $3)"
  ./dataSizeTest.sh $2 $3
else
  echo "Illegal Arguments"
  exit 1
fi