#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="YSB"
mainPath="com.madamaya.l3stream.workflows.ysb.L3YSB"
mainGLPath="com.madamaya.l3stream.workflows.ysb.GLYSB"
inputTopicName="YSB-i"
inputFilePath="${L3_HOME}/data/input/YSB/ysb.json"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=0