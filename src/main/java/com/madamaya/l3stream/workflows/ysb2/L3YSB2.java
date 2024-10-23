package com.madamaya.l3stream.workflows.ysb2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import com.madamaya.l3stream.workflows.ysb.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.LineageModeStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class L3YSB2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "YSB2";
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
        DataStream<L3StreamTupleContainer<YSBInputTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceYSB").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserYSBL3())).uid("3");
        DataStream<L3StreamTupleContainer<YSBInputTuple>> ds2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            ds2 = ds.map(L3.assignChkTs(new WatermarkStrategyYSB(), 0)).uid("4");
        } else {
            ds2 = ds.map(L3.extractInputTs(new WatermarkStrategyYSB())).uid("5");
        }
        DataStream<L3StreamTupleContainer<YSBResultTuple>> ds3 = ds2
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyYSB(), settings.readPartitionNum(env.getParallelism()))).uid("6")
                .filter(L3.filter(t -> t.getEventType().equals("view"))).uid("7")
                .map(L3.map(new ProjectAttributeYSBL3())).uid("8")
                .map(L3.mapTs(new TsAssignYSBL3())).uid("9")
                .keyBy(L3.keyBy(t -> t.getCampaignId(), String.class))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(L3.aggregateTs(new CountYSBL3())).uid("10");

        // L5
        Properties props = new Properties();
        if (L3.getClass() == LineageModeStrategy.class) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        }
        ds3.process(L3.extractTs()).uid("11").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode()).uid("12");

        env.execute(settings.getLineageMode() + ": " + queryFlag);
    }
}
