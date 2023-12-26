package com.madamaya.l3stream.workflows.ysb2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdataStringGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTupleGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTupleGL;
import com.madamaya.l3stream.workflows.ysb.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
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

public class GLYSB2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();
        // env.getCheckpointConfig().disableCheckpointing();

        final String queryFlag = "YSB2";
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
        //DataStream<YSBResultTupleGL> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<YSBResultTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceYSB")
                .map(new InitGLdataStringGL(settings))
                .map(new DataParserYSBGL())
                .assignTimestampsAndWatermarks(new WatermarkStrategyYSBGL())
                .filter(t -> t.getEventType().equals("view"))
                .map(new ProjectAttributeYSBGL())
                //.keyBy(t -> t.getCampaignId())
                .keyBy(new KeySelector<YSBInternalTupleGL, Integer>() {
                    int key = 0;
                    @Override
                    public Integer getKey(YSBInternalTupleGL ysbInternalTupleGL) throws Exception {
                        key = (key + 1) % 20;
                        return key;
                    }
                })
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.seconds(30))))
                .aggregate(new CountYSBGL(settings.aggregateStrategySupplier()));

        KafkaSink<YSBResultTupleGL> sink;
        Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<YSBResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageKafkaSinkYSBGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<YSBResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LatencyKafkaSinkYSBGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: GL" + queryFlag);
    }
}
