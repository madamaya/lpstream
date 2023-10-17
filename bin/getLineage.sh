#!/bin/zsh

source ./config.sh

# $1: main path, $2: JobID, $3: timestampOfOutputTuple, $4: lineageTopicName, $5: maxWindowSize,
# $6: valueOfOutputTuple, $7: parallelism, $8: numOfSourceOp, $9: redisIP, $10: redisPort
echo "java -cp ${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $7 $8 $9 ${10}"
java -cp ${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.TriggerReplay $1 $2 $3 $4 $5 $7 $8 $9 ${10}
echo "java -cp ${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.ReplayMonitor $3 $4 $6"
java -cp ${L3_HOME}/target/l3stream-1.0-SNAPSHOT.jar com.madamaya.l3stream.getLineage.ReplayMonitor $3 $4 $6

echo "END: getLineage.sh"
