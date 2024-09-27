package com.madamaya.l3stream.workflows.syn1;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import com.madamaya.l3stream.workflows.syn1.ops.LatencyKafkaSinkSyn1GLV2;
import com.madamaya.l3stream.workflows.syn1.ops.LineageKafkaSinkSyn1GLV2;
import com.madamaya.l3stream.workflows.syn1.ops.TempParserSynGL;
import com.madamaya.l3stream.workflows.syn1.ops.WatermarkStrategyTempSynGL;
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

public class GLSyn1 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn1";
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
        DataStream<SynTempTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn1")
                .map(new TempParserSynGL(settings))
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSynGL());

        KafkaSink<SynTempTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynTempTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkSyn1GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynTempTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkSyn1GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
