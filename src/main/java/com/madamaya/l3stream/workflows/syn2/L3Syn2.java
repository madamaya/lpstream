package com.madamaya.l3stream.workflows.syn2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import com.madamaya.l3stream.workflows.syn1.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class L3Syn2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        DataStream<KafkaInputString> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn2").uid("1");
        DataStream<L3StreamTupleContainer<SynTempTuple>> temp = ds
                .map(L3.initMap(settings, 0)).uid("2")
                .map(L3.map(new TempParserSynL3())).uid("3")
                .filter(L3.filter(t -> t.getType() == 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn(), settings.readPartitionNum(env.getParallelism()))).uid("5");

        DataStream<L3StreamTupleContainer<SynPowerTuple>> power = ds
                .map(L3.initMap(settings, 1)).uid("6")
                .map(L3.map(new PowerParserSynL3())).uid("7")
                .filter(L3.filter(t -> t.getType() == 1)).uid("8")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyPowerSyn(), settings.readPartitionNum(env.getParallelism()))).uid("9");

        DataStream<L3StreamTupleContainer<SynJoinedTuple>> joined = power.keyBy(L3.keyBy(new KeySelector<SynPowerTuple, Integer>() {
            @Override
            public Integer getKey(SynPowerTuple synPowerTuple) throws Exception {
                return synPowerTuple.getMachineId();
            }
        }, Integer.class))
        .intervalJoin(temp.keyBy(L3.keyBy(new KeySelector<SynTempTuple, Integer>() {
            @Override
            public Integer getKey(SynTempTuple synTempTuple) throws Exception {
                return synTempTuple.getMachineId();
            }
        }, Integer.class)))
        .between(Time.milliseconds(0), Time.milliseconds(1000))
        .process(L3.processJoin(new ProcessJoinSynL3())).uid("10");

        if (settings.isInvokeCpAssigner()) {
            joined.map(new CpAssigner<>()).uid("11").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        } else {
            joined.sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        }

        env.execute("Query: " + queryFlag);
    }
}
