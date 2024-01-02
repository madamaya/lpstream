package com.madamaya.l3stream.workflows.syn1;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import com.madamaya.l3stream.workflows.syn1.ops.LatencyKafkaSinkSyn1V2;
import com.madamaya.l3stream.workflows.syn1.ops.OutputKafkaSinkSyn1V2;
import com.madamaya.l3stream.workflows.syn1.ops.TempParserSyn;
import com.madamaya.l3stream.workflows.syn1.ops.WatermarkStrategyTempSyn;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Syn1 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn1";
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
        DataStream<SynTempTuple> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn1")
                .map(new TempParserSyn(settings))
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn());

        KafkaSink<SynTempTuple> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynTempTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new OutputKafkaSinkSyn1V2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynTempTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkSyn1V2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
