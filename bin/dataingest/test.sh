#!/bin/zsh

source $(dirname $0)/../config.sh

filePath="/Users/yamada-aist/workspace/l3stream/data/input/data/lr.csv"
qName="LR"
topic="Test"
throughput=100000
granularity=1

./ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity}
./maxIngestRate.sh ${filePath} ${qName} ${topic} ${parallelism}
