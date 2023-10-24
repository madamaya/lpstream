package com.madamaya.l3stream.workflows.nyc;

import com.madamaya.l3stream.glCommons.InitGdataGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import com.madamaya.l3stream.workflows.nyc.ops.*;
import com.madamaya.l3stream.workflows.ysb.ops.LineageKafkaSinkYSBGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class GLNYC {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(4);
        final String queryFlag = "NYC";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        boolean local = true;
        Properties kafkaProperties = new Properties();
        if (local) {
            kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        } else {
            kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        }
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<NYCResultTupleGL> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new InitGdataGL(settings))
                .map(new DataParserNYCGL())
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYCGL())
                .filter(t -> t.getTripDistance() > 5)
                .keyBy(new KeySelector<NYCInputTupleGL, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTupleGL tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .aggregate(new CountAndAvgDistanceGL(settings.aggregateStrategySupplier()));

        if (settings.getLatencyFlag() == 1) {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new LineageKafkaSinkNYCGL(outputTopicName, settings), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        } else {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new LatencyKafkaSinkNYCGL(outputTopicName, settings), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }

        /*
        ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NYCResultTupleGL>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(NYCResultTupleGL tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
         */

        env.execute("Query: " + queryFlag);
    }
}
