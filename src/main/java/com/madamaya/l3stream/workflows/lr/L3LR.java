package com.madamaya.l3stream.workflows.lr;

import com.madamaya.l3stream.cpstore.CpManagerClient;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.lr.ops.DataParserLR;
import com.madamaya.l3stream.workflows.lr.ops.WatermarkStrategyLR;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
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
        final String outputTopicName = queryFlag + "-o";

        boolean local = true;
        Properties kafkaProperties = new Properties();
        if (local) {
            kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        } else {
            kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        }
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<L3StreamTupleContainer<CountTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
                .map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings)).uid("2")
                .map(L3.map(new DataParserLR())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyLR(), settings.getRedisIp(), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyLR(), settings.numOfInstanceWM())).uid("5")
                .filter(L3.filter(t -> t.getType() == 0 && t.getSpeed() == 0)).uid("6")
                .keyBy(L3.keyBy(t -> t.getKey(), String.class))
                .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
                        STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(L3.aggregate(new LinearRoadVehicleAggregate())).uid("7")
                .filter(L3.filter(t -> t.getReports() == 4 && t.isUniquePosition())).uid("8")
                .keyBy(L3.keyBy(t -> t.getLatestPos(), Integer.class))
                .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE,
                        ACCIDENT_WINDOW_SLIDE))
                .aggregate(L3.aggregate(new LinearRoadAccidentAggregate())).uid("9")
                .filter(L3.filter(t -> t.getCount() > 1)).uid("10");

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            ds.map(new CpAssigner<>()).uid("11").addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("12");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            ds.map(new CpAssigner<>()).uid("13").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
        }

        if (settings.cpmProcessing()) {
            DataStream<ObjectNode> ds2 = env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("100").setParallelism(1)
                    .map(new CpManagerClient(settings)).uid("101").setParallelism(1);
        }

        env.execute("Query: " + queryFlag);
    }
}
