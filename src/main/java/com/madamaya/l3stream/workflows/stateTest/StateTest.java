package com.madamaya.l3stream.workflows.stateTest;

import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class StateTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "StateTest";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<Tuple2<Integer, Integer>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
                .map(new MapFunction<ObjectNode, Integer>() {
                    @Override
                    public Integer map(ObjectNode s) throws Exception {
                        return s.get("value").asInt();
                    }
                })
                .keyBy(t -> t)
                .map(new RichMapFunction<Integer, Tuple2<Integer, Integer>>() {
                    transient ValueState<Integer> cnt;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("test", Integer.class, 0);
                        cnt = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple2<Integer, Integer> map(Integer tuple) throws Exception {
                        int ccnt = cnt.value() + 1;
                        cnt.update(ccnt);
                        return Tuple2.of(tuple, ccnt);
                    }
                });

        ds.print();
        ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<Tuple2<Integer, Integer>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<Integer, Integer> tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("StateTest.java");
    }
}
