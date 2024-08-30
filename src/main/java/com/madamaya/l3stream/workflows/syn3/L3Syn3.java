package com.madamaya.l3stream.workflows.syn3;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import com.madamaya.l3stream.workflows.syn1.ops.AvgTemperatureL3;
import com.madamaya.l3stream.workflows.syn1.ops.TempParserSynL3;
import com.madamaya.l3stream.workflows.syn1.ops.TsAssignTempMapL3;
import com.madamaya.l3stream.workflows.syn1.ops.WatermarkStrategyTempSyn;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class L3Syn3 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn3";
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
        DataStream<L3StreamTupleContainer<SynResultTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn1").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new TempParserSynL3())).uid("3")
                .filter(L3.filter(t -> t.getType() == 0)).uid("4")
                .map(L3.updateTsWM(new WatermarkStrategyTempSyn(), 0)).uid("5")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn(), settings.readPartitionNum(env.getParallelism()))).uid("6")
                .map(L3.mapTs(new TsAssignTempMapL3())).uid("TsAssignTempMapL3")
                .keyBy(L3.keyBy(t -> t.getMachineId(), Integer.class))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(L3.aggregateTs(new AvgTemperatureL3())).uid("7");

        Properties props = new Properties();
        if (settings.getLineageMode() == "LineageMode") {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        }
        if (settings.isInvokeCpAssigner()) {
            ds.map(new CpAssigner<>()).uid("8").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode());
        } else {
            ds.sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode());
        }

        env.execute("Query: " + queryFlag);
    }
}
