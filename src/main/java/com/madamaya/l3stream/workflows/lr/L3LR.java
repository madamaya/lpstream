package com.madamaya.l3stream.workflows.lr;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.lr.ops.DataParserLRL3;
import com.madamaya.l3stream.workflows.lr.ops.WatermarkStrategyLR;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class L3LR {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();
        if (settings.getLineageMode() == "LineageMode") {
            // env.getCheckpointConfig().disableCheckpointing();
        }

        final String queryFlag = "LR";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
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
        //DataStream<L3StreamTupleContainer<CountTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<L3StreamTupleContainer<LinearRoadInputTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceLR").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserLRL3())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyLR(), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyLR(), settings.readPartitionNum(env.getParallelism()))).uid("5");

        // L5
        if (settings.isInvokeCpAssigner()) {
            ds.map(new CpAssigner<>()).uid("11").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        } else {
            ds.sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        }
        /*
        if (settings.getLineageMode() == "NonLineageMode") {
            if (settings.isInvokeCpAssigner()) {
                ds.map(new CpAssigner<>()).uid("11").sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("12");
            } else {
                ds.sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("13");
            }
        } else {
            if (settings.isInvokeCpAssigner()) {
                ds.map(new CpAssigner<>()).uid("14").sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("15");
            } else {
                ds.sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("16");
            }
        }
         */

        /*
        if (settings.cpmProcessing()) {
            KafkaSource<L3StreamInput<String>> tempSource = KafkaSource.<L3StreamInput<String>>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(inputTopicName)
                    .setGroupId("tempSource")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setDeserializer(new StringL3DeserializerV2())
                    .build();

            DataStream ds2 = env.fromSource(tempSource, WatermarkStrategy.noWatermarks(), "tempSource").uid("100").setParallelism(1)
                    .map(new CpManagerClient()).uid("101").setParallelism(1);
        }
         */

        env.execute(settings.getLineageMode() + "," + queryFlag);
    }
}
