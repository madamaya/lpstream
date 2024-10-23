package com.madamaya.l3stream.workflows.syn1;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import com.madamaya.l3stream.workflows.syn1.ops.TempParserSynL3;
import com.madamaya.l3stream.workflows.syn1.ops.WatermarkStrategyTempSyn;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class L3Syn1 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn1";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(settings.setOffsetsInitializer())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        // Source & InitMap & Parse & Filter
        DataStream<L3StreamTupleContainer<SynTempTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn1").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new TempParserSynL3())).uid("3")
                .filter(L3.filter(t -> t.getType() == 0)).uid("4");
        // Additional operator (assignChkTs or extractInputTs)
        DataStream<L3StreamTupleContainer<SynTempTuple>> ds2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            ds2 = ds.map(L3.assignChkTs(new WatermarkStrategyTempSyn(), 0)).uid("5");
        } else {
            ds2 = ds.map(L3.extractInputTs(new WatermarkStrategyTempSyn())).uid("6");
        }
        // Main process
        DataStream<L3StreamTupleContainer<SynTempTuple>> ds3 = ds2
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn(), settings.readPartitionNum(env.getParallelism()))).uid("7");

        // Sink
        ds3.process(L3.extractTs()).uid("8").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());

        env.execute(settings.getLineageMode() + ": " + queryFlag);
    }
}
