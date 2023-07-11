package com.madamaya.l3stream.samples.lr.test1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class Test1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");

        String inputTopicName = "input";
        env
                .addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .filter(t -> true);

        env.execute("Test1");
    }
}
