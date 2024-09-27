package com.madamaya.l3stream.workflows.syn2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import com.madamaya.l3stream.workflows.syn1.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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
                .setStartingOffsets(settings.setOffsetsInitializer())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        // Source
        DataStream<KafkaInputString> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn2").uid("1");
        // InitMap & Parse & Filter
        DataStream<L3StreamTupleContainer<SynTempTuple>> temp = ds
                .map(L3.initMap(settings, 0)).uid("2")
                .map(L3.map(new TempParserSynL3())).uid("3")
                .filter(L3.filter(t -> t.getType() == 0)).uid("4");
        // Additional operator (assignChkTs or extractInputTs)
        DataStream<L3StreamTupleContainer<SynTempTuple>> temp2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            temp2 = temp.map(L3.assignChkTs(new WatermarkStrategyTempSyn(), 0)).uid("5");
        } else {
            temp2 = temp.map(L3.extractInputTs(new WatermarkStrategyTempSyn())).uid("6");
        }
        // Main process
        DataStream<L3StreamTupleContainer<SynTempTuple>> temp3 = temp2
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn(), settings.readPartitionNum(env.getParallelism()))).uid("7")
                .map(L3.mapTs(new TsAssignTempMapL3())).uid("8");

        // InitMap & Parse & Filter
        DataStream<L3StreamTupleContainer<SynPowerTuple>> power = ds
                .map(L3.initMap(settings, 1)).uid("9")
                .map(L3.map(new PowerParserSynL3())).uid("10")
                .filter(L3.filter(t -> t.getType() == 1)).uid("11");
        // Additional operator (assignChkTs or extractInputTs)
        DataStream<L3StreamTupleContainer<SynPowerTuple>> power2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            power2 = power.map(L3.assignChkTs(new WatermarkStrategyPowerSyn(), 1)).uid("12");
        } else {
            power2 = power.map(L3.extractInputTs(new WatermarkStrategyPowerSyn())).uid("13");
        }
        // Main process
        DataStream<L3StreamTupleContainer<SynPowerTuple>> power3 = power2
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyPowerSyn(), settings.readPartitionNum(env.getParallelism()))).uid("14")
                .map(L3.mapTs(new TsAssignPowerMapL3())).uid("15");

        // Main process
        DataStream<L3StreamTupleContainer<SynJoinedTuple>> joined = power3.join(temp3)
                .where(L3.keyBy(new KeySelector<SynPowerTuple, Integer>() {
                    @Override
                    public Integer getKey(SynPowerTuple synPowerTuple) throws Exception {
                        return synPowerTuple.getMachineId();
                    }
                }, Integer.class))
                .equalTo(L3.keyBy(new KeySelector<SynTempTuple, Integer>() {
                    @Override
                    public Integer getKey(SynTempTuple synTempTuple) throws Exception {
                        return synTempTuple.getMachineId();
                    }
                }, Integer.class))
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .with(L3.joinTs(new JoinSynL3())).uid("16");

        // Sink
        joined.process(L3.extractTs()).uid("17").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());

        env.execute(settings.getLineageMode() + ": " + queryFlag);
    }
}
