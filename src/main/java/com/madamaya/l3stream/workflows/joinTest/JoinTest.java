package com.madamaya.l3stream.workflows.joinTest;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class JoinTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "JoinTest";
        final String inputTopicName = queryFlag + "1-i";
        final String inputTopicName2 = queryFlag + "2-i";
        final String outputTopicName = queryFlag + "-o";

        Properties kafkaProperties = new Properties();
        Properties kafkaProperties2 = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties2.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties2.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        kafkaProperties2.setProperty("transaction.timeout.ms", "540000");

        env.setParallelism(1);

        /* Query */
        DataStream<Tuple3<Integer, String, Long>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
            .map(new MapFunction<ObjectNode, Tuple3<Integer, String, Long>>() {
                @Override
                public Tuple3<Integer, String, Long> map(ObjectNode jsonNodes) throws Exception {
                    String[] es = jsonNodes.get("value").asText().split(",");
                    return Tuple3.of(Integer.parseInt(es[0]), es[1], Long.parseLong(es[2]));
                }
            }).assignTimestampsAndWatermarks(new WatermarkStrategy1());
        ds.print();

        /* Query */
        DataStream<Tuple3<Integer, Integer, Long>> ds2 = env.addSource(new FlinkKafkaConsumer<>(inputTopicName2, new JSONKeyValueDeserializationSchema(true), kafkaProperties2).setStartFromLatest())
                .map(new MapFunction<ObjectNode, Tuple3<Integer, Integer, Long>>() {
                    @Override
                    public Tuple3<Integer, Integer, Long> map(ObjectNode jsonNodes) throws Exception {
                        String[] es = jsonNodes.get("value").asText().split(",");
                        return Tuple3.of(Integer.parseInt(es[0]), Integer.parseInt(es[1]), Long.parseLong(es[2]));
                    }
                }).assignTimestampsAndWatermarks(new WatermarkStrategy2());

        ds2.print();

        DataStream<Tuple3<Integer, Integer, String>> result = ds.join(ds2)
                .where(t -> t.f0)
                .equalTo(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1500)))
                .apply(new JoinFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public Tuple3<Integer, Integer, String> join(Tuple3<Integer, String, Long> ds1tuple, Tuple3<Integer, Integer, Long> ds2tuple) throws Exception {
                        return Tuple3.of(ds2tuple.f0, ds2tuple.f1, ds1tuple.f1);
                    }
                });

        result.print();
        result.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<Tuple3<Integer, Integer, String>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple3<Integer, Integer, String> tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("StateTest.java");
    }

}
