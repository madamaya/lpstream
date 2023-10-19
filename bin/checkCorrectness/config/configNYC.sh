#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="NYC"
mainPath="com.madamaya.l3stream.workflows.nyc.L3NYC"
inputTopicName="NYC-i"
inputFilePath="${L3_HOME}/data/input/NYC/yellow.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=1