#!/bin/zsh

source $(dirname $0)/../config.sh
source ../utils/dataIngest.sh

filePath="/Users/yamada-aist/workspace/l3stream/data/input/data/lr.csv.100000"
qName="LR"
topic="Test"
throughput=100000
granularity=1

ingestData ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity}
#maxIngestionRate ${filePath} ${qName} ${topic} ${parallelism}
