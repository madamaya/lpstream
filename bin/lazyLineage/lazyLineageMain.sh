#!/bin/zsh

source ./configNYC.sh
#source ./configYSB.sh
#source ./configLR.sh
#source ./configNexmark.sh

outputTopic="${testName}-l"

initDir=`pwd`

# generate target tuples
cd ../sampling
#./sampling.sh ${testName}

# get jobid
jobid=`cat jobid.txt`

cd ${initDir}

FILE="../sampling/log/${testName}/${testName}.log.sampled.txt"
while read LINE
do
  outputValue=`echo ${LINE} | jq '.OUT'`
  outputTs=`echo ${LINE} | jq '.TS' | sed -e 's/"//g'`

  cd ..
  echo "./getLineage.sh ${jobid} ${outputTs} ${outputTopic} ${maxWindowSize} ${outputValue}"
  ./getLineage.sh ${jobid} ${outputTs} ${outputTopic} ${maxWindowSize} ${outputValue}
  cd ${initDir}
done < ${FILE}