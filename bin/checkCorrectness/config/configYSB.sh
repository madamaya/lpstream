#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="YSB"
mainPath="com.madamaya.l3stream.workflows.ysb.L3YSB"
inputTopicName="YSB-i"
inputFilePath="${L3_HOME}/data/input/YSB/streaming-benchmarks/data/util/joined_input_10.json"
cmpPythonName="cmpBaselineAndReplay.py"