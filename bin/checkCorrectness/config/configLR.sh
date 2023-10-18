#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="LR"
mainPath="com.madamaya.l3stream.workflows.lr.L3LR"
inputTopicName="LR-i"
inputFilePath="${L3_HOME}/data/input/LinearRoad/h1_01.csv"
cmpPythonName="cmpBaselineAndReplay.py"