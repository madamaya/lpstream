package com.madamaya.l3stream.workflows.syn3;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdataStringGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynResultTupleGL;
import com.madamaya.l3stream.workflows.syn1.ops.AvgTemperatureGL;
import com.madamaya.l3stream.workflows.syn1.ops.TempParserSynGL;
import com.madamaya.l3stream.workflows.syn1.ops.TsAssignTempMapGL;
import com.madamaya.l3stream.workflows.syn1.ops.WatermarkStrategyTempSynGL;
import com.madamaya.l3stream.workflows.syn3.ops.LatencyKafkaSinkSyn3GLV2;
import com.madamaya.l3stream.workflows.syn3.ops.LineageKafkaSinkSyn3GLV2;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
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

public class GLSyn3 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn3";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        //DataStream<CountTuple> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<SynResultTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn1")
                .map(new InitGLdataStringGL(settings))
                .map(new TempParserSynGL())
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSynGL())
                .map(new TsAssignTempMapGL())
                .keyBy(t -> t.getMachineId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AvgTemperatureGL(settings.aggregateStrategySupplier()));

        KafkaSink<SynResultTupleGL> sink;
        Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageKafkaSinkSyn3GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynResultTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LatencyKafkaSinkSyn3GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
