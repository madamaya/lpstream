#!/bin/zsh

source ../config.sh
#source ./configNYC.sh
#source ./configYSB.sh
#source ./configLR.sh
#source ./configNexmark.sh

outputTopic="${testName}-l"

initDir=`pwd`

# generate target tuples
cd ../sampling
echo "./sampling.sh ${testName} ${mainPath}"
./sampling.sh ${testName} ${mainPath}

# get jobid
jobid=`cat jobid.txt`

cd ${initDir}

FILE="../sampling/log/${testName}/${testName}.log.sampled.txt"
while read LINE
do
  outputValue=`echo ${LINE} | jq '.OUT'`
  outputTs=`echo ${LINE} | jq '.TS' | sed -e 's/"//g'`

  cd ..
  echo "./getLineage.sh ${mainPath} ${jobid} ${outputTs} ${outputTopic} ${maxWindowSize} ${outputValue} ${parallelism} ${numOfSourceOp} ${redisIP} ${redisPort}"
  ./getLineage.sh ${mainPath} ${jobid} ${outputTs} ${outputTopic} ${maxWindowSize} ${outputValue} ${parallelism} ${numOfSourceOp} ${redisIP} ${redisPort}
  cd ${initDir}
done < ${FILE}