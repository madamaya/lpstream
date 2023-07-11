package com.madamaya.l3stream.workflows.linearroad.noprovenance.qs;

import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.LrWatermark;
import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.LatencyLoggingSink;
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

public class LinearRoadAccident {


  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setParallelism(4);

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
        .assignTimestampsAndWatermarks(new LrWatermark().withTimestampAssigner((t, l) -> Time.seconds(t.getTimestamp()).toMilliseconds())).disableChaining()
        .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
        .keyBy(t -> t.getKey())
        .window(SlidingEventTimeWindows.of(STOPPED_VEHICLE_WINDOW_SIZE,
            STOPPED_VEHICLE_WINDOW_SLIDE))
        .aggregate(new LinearRoadVehicleAggregate())
        .filter(t -> t.getReports() == 4 && t.isUniquePosition())
        .keyBy(t -> t.getLatestPos())
        .window(SlidingEventTimeWindows.of(ACCIDENT_WINDOW_SIZE,
            ACCIDENT_WINDOW_SLIDE))
        .aggregate(new LinearRoadAccidentAggregate())
        // .slotSharingGroup(settings.secondSlotSharingGroup())
        .filter(t -> t.getCount() > 1)
        .addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<CountTuple>() {
          @Override
          public ProducerRecord<byte[], byte[]> serialize(CountTuple countTuple, @Nullable Long aLong) {
            return new ProducerRecord<>(outputTopicName, countTuple.toString().getBytes(StandardCharsets.UTF_8));
          }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        //.addSink(LatencyLoggingSink.newInstance(settings))
        //.setParallelism(settings.sinkParallelism());

    env.execute("LinearRoadAccident");

  }
}
