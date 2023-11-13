#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="Nexmark"
mainPath="com.madamaya.l3stream.workflows.nexmark.L3Nexmark"
mainGLPath="com.madamaya.l3stream.workflows.nexmark.GLNexmark"
inputTopicName="Nexmark-i"
inputFilePath="${L3_HOME}/data/input/data/nexmark.json"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=2
aggregateStrategy="unsortedPtr"
