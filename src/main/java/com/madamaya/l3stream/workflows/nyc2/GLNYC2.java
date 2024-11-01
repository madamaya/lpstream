package com.madamaya.l3stream.workflows.nyc2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import com.madamaya.l3stream.workflows.nyc.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2GL;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
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
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class GLNYC2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "NYC2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputStringGL> source = KafkaSource.<KafkaInputStringGL>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2GL())
                .build();

        /* Query */
        DataStream<NYCResultTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNYC")
                .map(new DataParserNYCGL(settings))
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYCGL())
                .filter(t -> t.getTripDistance() > 5)
                .map(new TsAssignNYCGL())
                .keyBy(new KeySelector<NYCInputTupleGL, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTupleGL tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .aggregate(new CountAndAvgDistanceGL(settings.aggregateStrategySupplier()));

        KafkaSink<NYCResultTupleGL> sink;
        Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NYCResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageKafkaSinkNYCGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 100) {
            sink = KafkaSink.<NYCResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new OutputKafkaSinkNYCGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NYCResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LatencyKafkaSinkNYCGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: GL" + queryFlag);
    }
}
