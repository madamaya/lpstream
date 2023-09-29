package com.madamaya.l3stream.tests;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class RichFlatMapTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        String inputTopicName = "rfmap";
        String outputTopicName = "rfmapout";

        env.setParallelism(1);

        DataStream<String> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
                .map(new DataParser())
                .flatMap(new MyRichFlatMap());

        ds.print();

        ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, s.getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("output");
    }

    public static class DataParser implements MapFunction<ObjectNode, String> {

        @Override
        public String map(ObjectNode jsonNodes) throws Exception {
            return jsonNodes.get("value").textValue();
        }
    }

    public static class MyRichFlatMap extends RichFlatMapFunction<String, String> {
        BufferedWriter bw;
        int count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            count = 0;
            try {
                bw = new BufferedWriter(new FileWriter("/Users/yamada-aist/workspace/l3stream/data/test0918/" + System.currentTimeMillis() + ".log"));
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String out = s;
            count++;
            bw.write(count + "\n");
            bw.flush();
            collector.collect(out);
        }
    }

    public static class MyRichFlatMapWithState extends RichFlatMapFunction<String, String> {


        BufferedWriter bw;
        ValueState<Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> desc =
                    new ValueStateDescriptor<>(
                            "count",
                            Integer.class,
                            0
                    );
            count = getRuntimeContext().getState(desc);
            try {
                bw = new BufferedWriter(new FileWriter("/Users/yamada-aist/workspace/l3stream/data/test0918/" + System.currentTimeMillis() + ".log"));
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String out = s;
            count.update(count.value() + 1);
            bw.write(count.value() + "\n");
            bw.flush();
            collector.collect(out);
        }
    }
}
