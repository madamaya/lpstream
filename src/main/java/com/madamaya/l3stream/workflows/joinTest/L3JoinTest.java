package com.madamaya.l3stream.workflows.joinTest;

import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class L3JoinTest {
    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
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
        DataStream<L3StreamTupleContainer<Tuple3<Integer, String, Long>>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
                //.map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings))
            .map(L3.initMap(settings))
            .map(L3.map(new MapFunction<ObjectNode, Tuple3<Integer, String, Long>>() {
                @Override
                public Tuple3<Integer, String, Long> map(ObjectNode jsonNodes) throws Exception {
                    String[] es = jsonNodes.get("value").asText().split(",");
                    return Tuple3.of(Integer.parseInt(es[0]), es[1], Long.parseLong(es[2]));
                }
            })).assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategy1(), 1));
        // ds.print();

        /* Query */
        DataStream<L3StreamTupleContainer<Tuple3<Integer, Integer, Long>>> ds2 = env.addSource(new FlinkKafkaConsumer<>(inputTopicName2, new JSONKeyValueDeserializationSchema(true), kafkaProperties2).setStartFromLatest())
                //.map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings))
                .map(L3.initMap(settings))
                .map(L3.map(new MapFunction<ObjectNode, Tuple3<Integer, Integer, Long>>() {
                    @Override
                    public Tuple3<Integer, Integer, Long> map(ObjectNode jsonNodes) throws Exception {
                        String[] es = jsonNodes.get("value").asText().split(",");
                        return Tuple3.of(Integer.parseInt(es[0]), Integer.parseInt(es[1]), Long.parseLong(es[2]));
                    }
                })).assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategy2(), 1));
        // ds2.print();

        DataStream<L3StreamTupleContainer<Tuple3<Integer, Integer, String>>> result = ds.join(ds2)
                .where(L3.keyBy(t -> t.f0, Integer.class))
                .equalTo(L3.keyBy(t -> t.f1, Integer.class))
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1500)))
                .apply(L3.join(new JoinFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public Tuple3<Integer, Integer, String> join(Tuple3<Integer, String, Long> ds1tuple, Tuple3<Integer, Integer, Long> ds2tuple) throws Exception {
                        return Tuple3.of(ds2tuple.f0, ds2tuple.f1, ds1tuple.f1);
                    }
                }));
        // result.print();

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            result.addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("13");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            // result.addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
            result.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<L3StreamTupleContainer<Tuple3<Integer, Integer, String>>>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(L3StreamTupleContainer<Tuple3<Integer, Integer, String>> tuple, @Nullable Long aLong) {
                    return new ProducerRecord<>(outputTopicName, ("LIEAGEMODE: " +tuple.tuple().toString()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }
        /*
        result.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<Tuple3<Integer, Integer, String>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple3<Integer, Integer, String> tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
                 */

        env.execute("StateTest.java");
    }

}
