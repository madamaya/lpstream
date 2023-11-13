#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="LR"
mainPath="com.madamaya.l3stream.workflows.lr.L3LR"
mainGLPath="com.madamaya.l3stream.workflows.lr.GLLR"
inputTopicName="LR-i"
inputFilePath="${L3_HOME}/data/input/data/lr.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0
aggregateStrategy="sortedPtr"
