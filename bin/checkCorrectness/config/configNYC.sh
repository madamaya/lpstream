#!/bin/zsh

source $(dirname $0)/../../config.sh

testName="NYC"
mainPath="com.madamaya.l3stream.workflows.nyc.L3NYC"
mainGLPath="com.madamaya.l3stream.workflows.nyc.GLNYC"
inputTopicName="NYC-i"
inputFilePath="${L3_HOME}/data/input/data/nyc.csv"
cmpPythonName="cmpBaselineAndReplay.py"
parseFlag=1