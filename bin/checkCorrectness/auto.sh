#!/bin/zsh

source ../utils/flinkJob.sh

./main.sh 1 2>&1 | tee 1-1.log
restartFlinkCluster
./main.sh 3 2>&1 | tee 1-3.log
restartFlinkCluster
./main.sh 4 2>&1 | tee 1-4.log
restartFlinkCluster

./checkReplayOut.sh 1 2>&1 | tee 2-1.log
restartFlinkCluster
./checkReplayOut.sh 3 2>&1 | tee 2-3.log
restartFlinkCluster
./checkReplayOut.sh 4 2>&1 | tee 2-4.log
restartFlinkCluster

./main.sh 2 2>&1 | tee 1-2.log
restartFlinkCluster
./checkReplayOut.sh 2 2>&1 | tee 2-2.log
restartFlinkCluster
