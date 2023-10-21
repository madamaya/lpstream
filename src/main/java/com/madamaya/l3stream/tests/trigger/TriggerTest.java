package com.madamaya.l3stream.tests.trigger;

import com.madamaya.l3stream.tests.richmap.RichFlatMapTest;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.checkerframework.checker.units.qual.C;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class TriggerTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        String inputTopicName = "trigger";
        String outputTopicName = "triggerout";

        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest())
                .map(new DataParser())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Long> stringLongTuple2) {
                        return stringLongTuple2.f1-1;
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .trigger(new EventAndProcessingTimeTrigger())
                .aggregate(new Counter());

        ds.print();

        ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<Tuple2<String, Long>>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> tuple2, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, (tuple2.f0 + "," + tuple2.f1).getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("output");
    }

    public static class DataParser implements MapFunction<ObjectNode, Tuple2<String, Long>> {
        long cnt = 0;
        @Override
        public Tuple2<String, Long> map(ObjectNode jsonNodes) throws Exception {
            cnt = cnt + 100;
            return Tuple2.of(jsonNodes.get("value").textValue(), cnt);
        }
    }

    private static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // register system timer only for the first time
            ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, false);
            if (!firstTimerSet.value()) {
                ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
                firstTimerSet.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // schedule next timer
            ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
            return TriggerResult.FIRE;
        }
    }

    public static class Counter implements AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> t2, Tuple2<String, Long> acc) {
            acc.f0 = t2.f0;
            acc.f1++;
            return acc;
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {
            return Tuple2.of(acc.f0, acc.f1);
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
            throw new UnsupportedOperationException();
        }
    }
}
