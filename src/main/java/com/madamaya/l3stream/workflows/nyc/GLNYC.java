package com.madamaya.l3stream.workflows.nyc;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdata;
import com.madamaya.l3stream.glCommons.InitGdataJsonNodeGL;
import com.madamaya.l3stream.glCommons.InitGdataStringGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTupleGL;
import com.madamaya.l3stream.workflows.nexmark.ops.LatencyKafkaSinkNexmarkGLV2;
import com.madamaya.l3stream.workflows.nexmark.ops.LineageKafkaSinkNexmarkGLV2;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import com.madamaya.l3stream.workflows.nyc.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringL3DeserializerV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLinV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class GLNYC {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getCheckpointConfig().disableCheckpointing();

        final String queryFlag = "NYC";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        KafkaSource<L3StreamInput<String>> source = KafkaSource.<L3StreamInput<String>>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("myGroup")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new StringL3DeserializerV2())
                .build();

        /* Query */
        //DataStream<NYCResultTuple> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<NYCResultTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNYC")
                .map(new InitGLdata<>(settings))
                .map(new DataParserNYCGL())
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYCGL())
                .filter(t -> t.getTripDistance() > 5)
                .keyBy(new KeySelector<NYCInputTupleGL, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTupleGL tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                })
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.minutes(30))))
                .aggregate(new CountAndAvgDistanceGL(settings.aggregateStrategySupplier()));

        KafkaSink<NYCResultTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NYCResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkNYCGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NYCResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkNYCGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: GL" + queryFlag);
    }
}
