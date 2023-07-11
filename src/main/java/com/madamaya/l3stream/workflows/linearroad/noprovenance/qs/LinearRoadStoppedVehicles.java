package com.madamaya.l3stream.workflows.linearroad.noprovenance.qs;

import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.LatencyLoggingSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SIZE;
import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.STOPPED_VEHICLE_WINDOW_SLIDE;

public class LinearRoadStoppedVehicles {


  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    final String inputTopicName = "linearroadS-i";
    final String outputTopicName = "linearroadS-o";

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
      /*
      .assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<LinearRoadInputTuple>() {
          @Override
          public long extractAscendingTimestamp(LinearRoadInputTuple tuple) {
            return Time.seconds(tuple.getTimestamp()).toMilliseconds();
          }
        })
       */
      env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
        .map(new ObjectNodeConverter())
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .keyBy(t -> t.getKey())
        .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
            STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(v -> v.getReports() == 4 && v.isUniquePosition())
        .addSink(LatencyLoggingSink.newInstance(settings))
        .setParallelism(settings.sinkParallelism());

    env.execute("LinearRoadStoppedVehicles");
  }

}
