package com.madamaya.l3stream.workflows.nyc;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.ops.CountAndAvgDistance;
import com.madamaya.l3stream.workflows.nyc.ops.DataParserNYC;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

public class NYC {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "NYC";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<NYCResultTuple> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new DataParserNYC(settings))
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYC())
                .filter(t -> t.getTripDistance() > 5)
                .keyBy(new KeySelector<NYCInputTuple, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTuple tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                })
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.minutes(30))))
                .aggregate(new CountAndAvgDistance());

        if (settings.getLatencyFlag() == 1) {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NYCResultTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(NYCResultTuple tuple, @Nullable Long aLong) {
                    return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    // return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        } else {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NYCResultTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(NYCResultTuple tuple, @Nullable Long aLong) {
                    // return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }

        env.execute("Query: " + queryFlag);
    }
}
