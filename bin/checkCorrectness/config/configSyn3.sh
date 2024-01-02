#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="Syn3"
mainPath="com.madamaya.l3stream.workflows.syn3.Syn3"
mainL3Path="com.madamaya.l3stream.workflows.syn3.L3Syn3"
mainGLPath="com.madamaya.l3stream.workflows.syn3.GLSyn3"
inputTopicName="Syn3-i"
inputFilePath="${L3_HOME}/data/input/data/syn3.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0
aggregateStrategy="unsortedPtr"
