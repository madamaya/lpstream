package com.madamaya.l3stream.workflows.linearroad.noprovenance.wqs;

import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.LrWatermark;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.l3stream.cpm.CpManagerClient;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.VehicleTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.*;

public class LinearRoadAccidentShort {
  public static void main(String[] args) throws Exception {
    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    // env.setParallelism(4);

    // env.getCheckpointConfig().disableCheckpointing();
    ExperimentSettings settings = ExperimentSettings.newInstance(args);
    final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
    FlinkSerializerActivator.L3STREAM.activate(env, settings);

    final String inputTopicName = "linearroadA-i";
    final String outputTopicName = "linearroadA-o";

    boolean local = true;
    Properties kafkaProperties = new Properties();
    if (local) {
      kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
    } else {
      kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
    }
    kafkaProperties.setProperty("group.id", "myGROUP");
    kafkaProperties.setProperty("transaction.timeout.ms", "540000");

    // env.addSource(new LinearRoadSource(settings))
    DataStream<L3StreamTupleContainer<VehicleTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
        // .map(L3.initMap(t->System.nanoTime(), t->System.nanoTime(), settings, "Accident")).uid("2")
            .map(L3.initMap(settings)).uid("2")
        .map(L3.map(new ObjectNodeConverter())).uid("3")
        //.map(L3.updateTs(t->t.tuple().getTimestamp())).uid("3.5")
        .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new LrWatermark(), 1)).uid("4")
        .filter(L3.filter(t -> t.getType() == 0 && t.getSpeed() == 0)).uid("5")
        .keyBy(L3.keyBy(t -> t.getKey()), TypeInformation.of(String.class))
            .window(TumblingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE))
        //.window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
                //STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(L3.aggregate(new LinearRoadVehicleAggregate())).uid("6");

    // L5
    if (settings.getLineageMode() == "NonLineageMode") {
      ds.map(new CpAssigner<>()).uid("12")
              .addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("13");
    } else {
      ds.map(new CpAssigner<>()).uid("111").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
    }

    if (settings.cpmProcessing()) {
      DataStream<ObjectNode> ds2 =  env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("10").setParallelism(1)
              .map(new CpManagerClient()).uid("11").setParallelism(1);
    }

    env.execute("LinearRoadAccidentShort");

  }
}
