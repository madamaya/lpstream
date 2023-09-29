package com.madamaya.l3stream.workflows.linearroad.noprovenance.wqs;

import com.madamaya.l3stream.cpstore.CpManagerClient;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.LrWatermark;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.*;

public class LinearRoadAccident {
  public static void main(String[] args) throws Exception {
    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    // env.setParallelism(4);

    ExperimentSettings settings = ExperimentSettings.newInstance(args);
    final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings);

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
    DataStream<L3StreamTupleContainer<CountTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
        .map(L3.initMap(t->System.nanoTime(), t->System.nanoTime(), settings, "Accident")).uid("2")
        .map(L3.map(new ObjectNodeConverter())).uid("3")
        .map(L3.updateTs(t->t.tuple().getTimestamp())).uid("3.5")
        .assignTimestampsAndWatermarks(
                L3.assignTimestampsAndWatermarks(new LrWatermark()).withTimestampAssigner((t, l) -> Time.seconds(t.tuple().getTimestamp()).toMilliseconds())).uid("4")
        .filter(L3.filter(t -> t.getType() == 0 && t.getSpeed() == 0)).uid("5")
        .keyBy(L3.key(t -> t.getKey()), TypeInformation.of(String.class))
        .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
            STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(L3.aggregate(new LinearRoadVehicleAggregate())).uid("6")
        .filter(L3.filter(t -> t.getReports() == 4 && t.isUniquePosition())).uid("7")
        .keyBy(L3.key(t -> t.getLatestPos()), TypeInformation.of(Integer.class))
        .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE,
            ACCIDENT_WINDOW_SLIDE))
        .aggregate(L3.aggregate(new LinearRoadAccidentAggregate())).uid("8")
        // .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(L3.filter(t -> t.getCount() > 1)).uid("9");

    // L5
    if (settings.getLineageMode() == "NonLineageMode") {
      DataStream<ObjectNode> ds2 =  env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("10").setParallelism(1)
              .map(new CpManagerClient(settings)).uid("11").setParallelism(1);
      ds.map(new CpAssigner<>()).uid("12")
              .addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("13");
    } else {
      ds.addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
    }

    /*
    DataStreamSink dss = ds
        .addSink(LatencyLoggingSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());
         */

    env.execute("LinearRoadAccident");

  }
}
