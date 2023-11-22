#!/bin/zsh

source $(dirname $0)/../config.sh

#filePath="${L3_HOME}/data/input/data/lr.csv"
#qName="LR"
#filePath="${L3_HOME}/data/input/data/nexmark.json"
#qName="Nexmark"
#filePath="${L3_HOME}/data/input/data/nyc.csv"
#qName="NYC"
filePath="${L3_HOME}/data/input/data/ysb.json"
qName="YSB"

topic="Test-i"
throughput=1000
granularity=1

./ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
#./maxIngestRate.sh ${filePath} ${qName} ${topic} ${parallelism}
sleep 20
./stopIngestion.sh

#ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/ingestData.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity} &
#ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/maxIngestRate.sh ${filePath} ${qName} ${topic} ${parallelism} ${throughput} ${granularity}
#sleep 20
#ssh ${ingestNode} /bin/zsh ${L3_HOME}/bin/dataingest/stopIngestion.sh