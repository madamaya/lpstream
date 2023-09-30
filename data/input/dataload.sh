#!/bin/bash

function load() {
  # $1: datafile, $2: topicname
  echo "START: load"
  java -cp ${jarPath} ${dataloaderClass} $1 $2
  echo "END: load"
}

function loadLR() {
  echo "START: loadLR"
  load LinearRoad/h1_10.csv LR-i
  echo "END: loadLR"
}

function loadNYC() {
  echo "START: loadNYC"
  load NYC/yellow.csv NYC-i
  echo "END: loadNYC"
}

function loadYSB() {
  echo "START: loadYSB"
  #
  echo "END: loadYSB"
}

function loadSynthetic() {
  echo "START: loadSynthetic"
  load Synthetic/synDataset_key100000_size30000000.json Real-i
  load Synthetic/synDataset_key100000_size30000000.json Window-i
  echo "END: loadSynthetic"
}


jarPath="../../target/l3stream-1.0-SNAPSHOT.jar"
dataloaderClass="com.madamaya.l3stream.utils.DataLoader"
experiments=(Synthetic LR NYC YSB)
# experiments=(LR)

for experiment in ${experiments[@]}
do
  if [ ${experiment} == "Synthetic" ]
  then
    loadSynthetic
  elif [ ${experiment} == "LR" ]
  then
    loadLR
  elif [ ${experiment} == "NYC" ]
  then
    loadNYC
  elif [ ${experiment} == "YSB" ]
  then
    loadYSB
  else
    echo "ARGERROR: ${experiment}"
    exit 1
  fi
done
