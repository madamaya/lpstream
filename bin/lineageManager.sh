#!/bin/zsh

source ./config.sh
source ./utils/flinkJob.sh

mode=$1
<<COMMENT
if [ ${mode} = "normal" ]; then
  echo "normal mode has been deleted."
  exit 1

  if [ $# -ne 4 ]; then
    echo "Illegal Arguments (lineageManager.sh)"
    exit 1
  fi
  # $1: mode, $2: jar path, $3: main path, $4: numOfSourceOp
  jarPath=$2
  mainPath=$3
  numOfSourceOp=$4

  ## Initialize redis
  echo "*** Initialize redis ***"
  echo "(redis-cli FLUSHDB)"
  redis-cli FLUSHDB

  ## store jar path, main path, and numOfSourceOp
  echo "#!/bin/zsh" > lineageManagerConfig.sh
  echo "jarPath=\"${jarPath}\"" >> lineageManagerConfig.sh
  echo "mainPath=\"${mainPath}\"" >> lineageManagerConfig.sh
  echo "numOfSourceOp=\"${numOfSourceOp}\"" >> lineageManagerConfig.sh
  chmod 744 lineageManagerConfig.sh

  ## start checkpoint manager server
  echo "*** Start Checkpoint Management Server ***"
  echo "(./startCpMServer.sh > /dev/null &)"
  ./startCpMServer.sh > /dev/null &

  ## submit Flink job
  cd ${BIN_DIR}/templates
  echo "*** Submit Flink job ***"
  #echo "./nonlineage.sh ${jarPath} ${mainPath} ${cpmIP} ${cpmPort} ${redisIP} ${redisPort}"
  #./nonlineage.sh ${jarPath} ${mainPath} ${cpmIP} ${cpmPort} ${redisIP} ${redisPort}
  echo "(./nonlineage.sh ${jarPath} ${mainPath})"
  ./nonlineage.sh ${jarPath} ${mainPath}

  cd ${BIN_DIR}
  echo "*** Return sumitted jobid to the user by writing down the jobid to a file ***"
  echo "(jobid=\`getRunningJobID\`)"
  jobid=`getRunningJobID`
  echo "(echo ${jobid} > currentJobID.txt)"
  echo ${jobid} > currentJobID.txt

  echo "*** END of lineageManager.sh (normal mode) ***"
elif [ ${mode} = "lineage" ]; then
COMMENT
if [ $# -ne 7 ]; then
  echo "Illegal Arguments (lineageManager.sh)"
  exit 1
fi
# $1: jarPath, $2: mainPath, $3: jobid, $4: outputTs, $5: outputValue, $6: maxWsize, $7: lineageTopicName
jarPath=$1
mainPath=$2
jobid=$3
outputTs=$4
outputValue=$5
maxWindowSize=$6
lineageTopicName=$7
# define numOfSourceOp
if [[ ${mainPath} == *Nexmark* ]]; then
  numOfSourceOp=2
else
  numOfSourceOp=1
fi

echo "*** Start lineage derivation ***"
echo "(./getLineage.sh ${jarPath} ${mainPath} ${jobid} ${outputTs} ${lineageTopicName} ${maxWindowSize} ${outputValue} ${numOfSourceOp})"
./getLineage.sh ${jarPath} ${mainPath} ${jobid} ${outputTs} ${lineageTopicName} ${maxWindowSize} ${outputValue} ${numOfSourceOp}

echo "*** END of lineageManager.sh "
