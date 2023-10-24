package com.madamaya.l3stream.workflows.stateTest;

import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
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

public class L3StateTest {
    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();


        final String queryFlag = "StateTest";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<L3StreamTupleContainer<Tuple2<Integer, Integer>>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
                //.map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings))
                .map(L3.initMap(settings))
                .map(L3.map(new MapFunction<ObjectNode, Integer>() {
                    @Override
                    public Integer map(ObjectNode s) throws Exception {
                        return s.get("value").asInt();
                    }
                }))
                .keyBy(L3.keyBy(t -> t, Integer.class))
                .map(L3.richMap(new RichMapFunction<Integer, Tuple2<Integer, Integer>>() {
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
                }));

        ds.print();

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            ds.addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("13");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            ds.addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
        }

        /*
        ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<Tuple2<Integer, Integer>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<Integer, Integer> tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
         */

        env.execute("StateTest.java");
    }
}
