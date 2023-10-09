#!/bin/bash

# download nexmark
git clone https://github.com/nexmark/nexmark.git
cd nexmark/nexmark-flink
echo `pwd`
./build.sh

tar zxf nexmark-flink.tgz
mv nexmark-flink nexmark

# download flink
wget https://dlcdn.apache.org/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz
tar xzf flink-1.17.1-bin-scala_2.12.tgz
mv flink-1.17.1 flink

# download kafka
wget https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz
tar zxf kafka_2.12-3.5.1.tgz
mv kafka_2.12-3.5.1 kafka

# install additional flink library
cp nexmark/lib/nexmark-flink-0.2-SNAPSHOT.jar flink/lib
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
mv flink-sql-connector-kafka-1.17.1.jar flink/lib


# replace variables & generate datagen query
cd nexmark/queries
#cat ddl_gen.sql ddl_kafka.sql insert_kafka.sql > datagen.sql
sed -e 's/${TPS}/100000/g' \
    -e 's/${EVENTS_NUM}/0/g' \
    -e 's/${PERSON_PROPORTION}/1/g' \
    -e 's/${AUCTION_PROPORTION}/3/g' \
    -e 's/${BID_PROPORTION}/46/g' \
    ddl_gen.sql > ../../../../ddl_gen.sql

sed -e 's/${BOOTSTRAP_SERVERS}/localhost:9092/g' \
    ddl_kafka.sql > ../../../../ddl_kafka.sql

cp insert_kafka.sql ../../../../insert_kafka.sql

# start flink cluster
cd ../../flink
./bin/start-cluster.sh

# start kafka cluster
cd ../kafka
./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
sleep 5
./bin/kafka-server-start.sh -daemon ./config/server.properties
sleep 10
./bin/kafka-topics.sh --create --topic nexmark --bootstrap-server localhost:9092

# start datagen query
#cd ../flink
#./bin/sql-client.sh -f ../nexmark/queries/datagenReplaced.sql
