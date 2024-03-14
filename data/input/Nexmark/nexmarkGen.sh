#!/bin/zsh

source $(dirname $0)/../../../bin/config.sh

numTuples=27500000
if [ $# -eq 1 ]; then
  numTuples=$1
fi

echo "=*=*=*=*= Start Nexmark data generation =*=*=*=*="

Nexmark_HOME=`pwd`

# download flink
#echo "*** Download flink for Nexmark data generation ***"
#wget https://dlcdn.apache.org/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
#tar xzf flink-1.17.1-bin-scala_2.12.tgz
#mv flink-1.17.1 flink

# download kafka
#echo "*** Download kafka for Nexmark data generation ***"
#wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz
#tar zxf kafka_2.12-3.5.1.tgz
#mv kafka_2.12-3.5.1 kafka

# download nexmark
echo "*** Download nexmark and compile it ***"
git clone https://github.com/nexmark/nexmark.git
cd nexmark/nexmark-flink
echo `pwd`
./build.sh
tar zxf nexmark-flink.tgz
mv nexmark-flink nexmark

# Install additional flink library
echo "*** Install additional libraries for flink ***"
cp nexmark/lib/nexmark-flink-0.2-SNAPSHOT.jar ${FLINK_HOME}/lib
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
mv flink-sql-connector-kafka-1.17.1.jar ${FLINK_HOME}/lib

# replace variables & generate datagen query
echo "*** Generate queries for data generation ***"
cd ${Nexmark_HOME}
sed -e 's/${TPS}/100000/g' \
    -e 's/${EVENTS_NUM}/0/g' \
    -e 's/${PERSON_PROPORTION}/1/g' \
    -e 's/${AUCTION_PROPORTION}/3/g' \
    -e 's/${BID_PROPORTION}/46/g' \
    ./nexmark/nexmark-flink/nexmark/queries/ddl_gen.sql | \
tr '\n' ' ' > ./queries.sql
echo "" >> ./queries.sql
sed -e 's/${BOOTSTRAP_SERVERS}/localhost:9092/g' \
    ./nexmark/nexmark-flink/nexmark/queries/ddl_kafka.sql | \
tr '\n' ' ' >> ./queries.sql
echo "" >> ./queries.sql
sed -e '/^--/d' ./nexmark/nexmark-flink/nexmark/queries/insert_kafka.sql | tr '\n' ' ' >> ./queries.sql
echo "" >> ./queries.sql

# start flink cluster
echo "*** Start flink cluster ***"
${FLINK_HOME}/bin/start-cluster.sh

# start kafka cluster
echo "*** Start zookeeper for kafka ***"
echo "(${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties)"
${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
sleep 10
echo "*** Start kafka server ***"
echo "(${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties)"
${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${KAFKA_HOME}/config/server.properties
sleep 10
echo "*** Create kafka topic (nexmark) ***"
echo "(${KAFKA_HOME}/bin/kafka-topics.sh --create --topic nexmark --bootstrap-server localhost:9092)"
${KAFKA_HOME}/bin/kafka-topics.sh --create --topic nexmark --bootstrap-server localhost:9092

# start datagen query
echo "*** Run dataGen queries ***"
${FLINK_HOME}/bin/sql-client.sh < queries.sql

# start data logger
echo "*** Start data logger ***"
python kafkaLogger.py ${numTuples}

# stop flink cluster
echo "*** Stop flink cluster ***"
${FLINK_HOME}/bin/stop-cluster.sh

# remove additional flink libraries
echo "*** Remove additional flink libraries ***"
rm ${FLINK_HOME}/lib/nexmark-flink-0.2-SNAPSHOT.jar
rm ${FLINK_HOME}/lib/flink-sql-connector-kafka-1.17.1.jar

# stop kafka cluster
echo "*** Remove topic ***"
${KAFKA_HOME}/bin/kafka-topics.sh --delete --topic nexmark --bootstrap-server localhost:9092
sleep 10
echo "*** Stop kafka server ***"
${KAFKA_HOME}/bin/kafka-server-stop.sh
sleep 30
echo "*** Stop zookeeper ***"
${KAFKA_HOME}/bin/zookeeper-server-stop.sh

echo "cp ../data/nexmark.json ../data/nexmark2.json"
cp ../data/nexmark.json ../data/nexmark2.json

echo "=*=*=*=*= End Nexmark data generation =*=*=*=*="