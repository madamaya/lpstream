package com.madamaya.l3stream.workflows.ysb;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTupleGL;
import com.madamaya.l3stream.workflows.ysb.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2GL;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

public class GLYSB {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "YSB";
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
        DataStream<YSBResultTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceYSB")
                .map(new DataParserYSBGL(settings))
                .assignTimestampsAndWatermarks(new WatermarkStrategyYSBGL())
                .filter(t -> t.getEventType().equals("view"))
                .map(new ProjectAttributeYSBGL())
                .map(new TsAssignYSBGL())
                .keyBy(t -> t.getCampaignId())
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
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
        } else if (settings.getLatencyFlag() == 100) {
            sink = KafkaSink.<YSBResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new OutputKafkaSinkYSBGLV2(outputTopicName, settings))
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
