#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="Syn1"
mainPath="com.madamaya.l3stream.workflows.syn1.Syn1"
mainL3Path="com.madamaya.l3stream.workflows.syn1.L3Syn1"
mainGLPath="com.madamaya.l3stream.workflows.syn1.GLSyn1"
inputTopicName="Syn1-i"
inputFilePath="${L3_HOME}/data/input/data/syn1.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0
aggregateStrategy="unsortedPtr"
