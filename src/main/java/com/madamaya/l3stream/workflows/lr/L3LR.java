package com.madamaya.l3stream.workflows.lr;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.lr.ops.DataParserLR;
import com.madamaya.l3stream.workflows.lr.ops.DataParserLRL3;
import com.madamaya.l3stream.workflows.lr.ops.WatermarkStrategyLR;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.l3stream.cpm.CpManagerClient;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.LineageKafkaSinkV2;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSinkV2;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringL3DeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.*;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import javax.xml.crypto.Data;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.*;

public class L3LR {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "LR";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        KafkaSource<L3StreamInput<String>> source = KafkaSource.<L3StreamInput<String>>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("myGroup")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new StringL3DeserializerV2())
                .build();

        /* Query */
        //DataStream<L3StreamTupleContainer<CountTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<L3StreamTupleContainer<CountTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceLR").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserLRL3())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyLR(settings.maxParallelism()), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyLR(settings.maxParallelism()), settings.numOfInstanceWM())).uid("5")
                .filter(L3.filter(t -> t.getType() == 0 && t.getSpeed() == 0)).uid("6")
                .keyBy(L3.keyBy(t -> t.getKey(), String.class))
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(STOPPED_VEHICLE_WINDOW_SIZE),
                        STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(L3.aggregate(new LinearRoadVehicleAggregateL3())).uid("7")
                .filter(L3.filter(t -> t.getReports() == (4 * settings.getWindowSize()) && t.isUniquePosition())).uid("8")
                .keyBy(L3.keyBy(t -> t.getLatestPos(), Integer.class))
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(ACCIDENT_WINDOW_SIZE),
                        ACCIDENT_WINDOW_SLIDE))
                .aggregate(L3.aggregate(new LinearRoadAccidentAggregateL3())).uid("9")
                .filter(L3.filter(t -> t.getCount() > 1)).uid("10");

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            // ds.map(new CpAssigner<>()).uid("11").addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("12");
            ds.map(new CpAssigner<>()).uid("11").sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("12");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            // ds.map(new CpAssigner<>()).uid("13").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
            ds.map(new CpAssigner<>()).uid("13").sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("14");
        }

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
