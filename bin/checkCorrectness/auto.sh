#!/bin/zsh

./main.sh 1 2>&1 | tee 1-1.log
./main.sh 3 2>&1 | tee 1-3.log
./main.sh 4 2>&1 | tee 1-4.log

./checkReplayOut.sh 1 2>&1 | tee 2-1.log
./checkReplayOut.sh 3 2>&1 | tee 2-3.log
./checkReplayOut.sh 4 2>&1 | tee 2-4.log

./main.sh 2 2>&1 | tee 1-2.log
./checkReplayOut.sh 2 2>&1 | tee 2-2.log


