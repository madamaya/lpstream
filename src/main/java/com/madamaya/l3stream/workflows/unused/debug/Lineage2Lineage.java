package com.madamaya.l3stream.workflows.unused.debug;

import com.madamaya.l3stream.workflows.unused.debug.util.DataParser;
import com.madamaya.l3stream.workflows.unused.debug.util.LineageTraverser;
import com.madamaya.l3stream.workflows.unused.debug.util.Sum;
import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class Lineage2Lineage {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final ProvenanceFunctionFactory GL = new ProvenanceFunctionFactory(settings.aggregateStrategySupplier());
        FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings);

        final String inputTopicName = "l2l-i";
        final String outputTopicName = "l2l-o";

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        DataStream<ProvenanceTupleContainer<Tuple3<Integer, Integer, Integer>>> input = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
                .map(new DataParser())
                .map(GL.initMap(t-> Long.valueOf(t.f2), t->System.nanoTime())).uid("2")
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<ProvenanceTupleContainer<Tuple3<Integer, Integer, Integer>>>() {
                            @Override
                            public long extractAscendingTimestamp(ProvenanceTupleContainer<Tuple3<Integer, Integer, Integer>> t) {
                                return t.tuple().f2;
                            }
                        }
                );

        DataStream<ProvenanceTupleContainer<Tuple3<Integer, Integer, String>>> q1 = input
                .keyBy(GL.key(t -> t.f0, Integer.class))
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .aggregate(GL.aggregate(new Sum()));

        DataStream<ProvenanceTupleContainer<Tuple3<Integer, Integer, String>>> q2 = input
                .filter(t -> t.tuple().f1 > 0)
                .keyBy(GL.key(t -> t.f0, Integer.class))
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .aggregate(GL.aggregate(new Sum()));

        q1.map(new LineageTraverser(settings)).print();
        q2.map(new LineageTraverser(settings)).print();

        env.execute();
    }
}
