package com.madamaya.l3stream.workflows.linearroad.noprovenance.qs;

import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.LrWatermark;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.VehicleTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
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

public class LinearRoadAccidentShort {
  public static void main(String[] args) throws Exception {
    // ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(1);

    final String inputTopicName = "linearroadA-s";
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
    env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
        .map(new ObjectNodeConverter())
    /*
        .assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<LinearRoadInputTuple>() {
          @Override
          public long extractAscendingTimestamp(LinearRoadInputTuple tuple) {
            return Time.seconds(tuple.getTimestamp()).toMilliseconds();
          }
        })
     */
        .assignTimestampsAndWatermarks(new LrWatermark())
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .keyBy(t -> t.getKey())
        .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
            STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<VehicleTuple>() {
          @Override
          public ProducerRecord<byte[], byte[]> serialize(VehicleTuple vehicleTuple, @Nullable Long aLong) {
            String ret = "{\"OUT\":\"" + vehicleTuple.toString() + "\"}";
            return new ProducerRecord<>(outputTopicName, ret.getBytes(StandardCharsets.UTF_8));
          }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
    /*
    .addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<LinearRoadInputTuple>() {
      @Override
      public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTuple vehicleTuple, @Nullable Long aLong) {
        String ret = "{\"OUT\":\"" + vehicleTuple.toString() + "\"}";
        return new ProducerRecord<>(outputTopicName, ret.getBytes(StandardCharsets.UTF_8));
      }
    }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
    */

    env.execute("LinearRoadAccident");

  }
}
