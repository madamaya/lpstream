#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="LR2"
mainPath="com.madamaya.l3stream.workflows.lr2.L3LR2"
mainGLPath="com.madamaya.l3stream.workflows.lr2.GLLR2"
inputTopicName="LR2-i"
inputFilePath="${L3_HOME}/data/input/data/lr2.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0
aggregateStrategy="unsortedPtr"
