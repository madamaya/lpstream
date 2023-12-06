package com.madamaya.l3stream.workflows.nyc2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
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
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class NYC2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "NYC2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        /*
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
         */

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        //DataStream<NYCResultTuple> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<NYCResultTuple> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNYC")
                .map(new DataParserNYC(settings))
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYC())
                .filter(t -> t.getTripDistance() > 5)
                .keyBy(new KeySelector<NYCInputTuple, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTuple tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                })
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.seconds(30))))
                .aggregate(new CountAndAvgDistance());

        KafkaSink<NYCResultTuple> sink;
        Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NYCResultTuple>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new OutputKafkaSinkNYCV2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NYCResultTuple>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LatencyKafkaSinkNYCV2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);


        env.execute("Query: " + queryFlag);
    }
}
