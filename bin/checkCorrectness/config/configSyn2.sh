#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="Syn2"
mainPath="com.madamaya.l3stream.workflows.syn2.Syn2"
mainL3Path="com.madamaya.l3stream.workflows.syn2.L3Syn2"
mainGLPath="com.madamaya.l3stream.workflows.syn2.GLSyn2"
inputTopicName="Syn2-i"
inputFilePath="${L3_HOME}/data/input/data/syn2.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0
aggregateStrategy="unsortedPtr"
