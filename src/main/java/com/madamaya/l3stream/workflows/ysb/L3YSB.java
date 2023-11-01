package com.madamaya.l3stream.workflows.ysb;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import com.madamaya.l3stream.workflows.ysb.ops.*;
import io.palyvos.provenance.l3stream.cpm.CpManagerClient;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class L3YSB {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "YSB";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        /* Query */
        DataStream<L3StreamTupleContainer<YSBResultTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserYSBL3())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyYSB(), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyYSB(), settings.numOfInstanceWM())).uid("5")
                .filter(L3.filter(t -> t.getEventType().equals("view"))).uid("6")
                .map(L3.map(new ProjectAttributeYSBL3())).uid("7")
                .keyBy(L3.keyBy(t -> t.getCampaignId(), String.class))
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.seconds(10))))
                .aggregate(L3.aggregate(new CountYSBL3())).uid("8");

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            ds.map(new CpAssigner<>()).uid("9").addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("10");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            ds.map(new CpAssigner<>()).uid("11").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("12");
        }

        if (settings.cpmProcessing()) {
            DataStream<ObjectNode> ds2 = env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("100").setParallelism(1)
                    .map(new CpManagerClient()).uid("101").setParallelism(1);
        }

        env.execute(settings.getLineageMode() + "," + queryFlag);
    }
}
