package com.madamaya.l3stream.workflows.linearroad.noprovenance.qs.provenance;

import com.madamaya.l3stream.workflows.linearroad.noprovenance.utils.ObjectNodeConverter;
import io.palyvos.provenance.ananke.functions.ProvenanceFunctionFactory;
import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.VehicleTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import io.palyvos.provenance.util.TimestampConverter;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.function.Function;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.*;

public class LinearRoadCombinedShort {

  public static void main(String[] args) throws Exception {
    ExperimentSettings settings = ExperimentSettings.newInstance(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final TimestampConverter timestampConverter = (ts) -> Time.seconds(ts).toMilliseconds();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.setMaxParallelism(settings.maxParallelism());
    env.setParallelism(4);
    final ProvenanceFunctionFactory GL =
        new ProvenanceFunctionFactory(settings.aggregateStrategySupplier());

    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings);

  final String inputTopicName = "linearroadA-i";
  final String outputTopicName = "linearroadA-o";

  Properties kafkaProperties = new Properties();
  kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
  kafkaProperties.setProperty("group.id", "myGROUP");
  kafkaProperties.setProperty("transaction.timeout.ms", "540000");

      SingleOutputStreamOperator<ProvenanceTupleContainer<VehicleTuple>> sourceStream = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
            .map(new ObjectNodeConverter())
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<LinearRoadInputTuple>() {
                  @Override
                  public long extractAscendingTimestamp(LinearRoadInputTuple tuple) {
                    return timestampConverter.apply(tuple.getTimestamp());
                  }
                })
            .map(
                GL.initMap(
                    (Serializable & Function<LinearRoadInputTuple, Long>) t -> t.getTimestamp(),
                    (Serializable & Function<LinearRoadInputTuple, Long>) t -> t.getStimulus()))
            .map(settings.genealogActivator().uidAssigner(0, settings.maxParallelism()))
            .returns(new TypeHint<ProvenanceTupleContainer<LinearRoadInputTuple>>() {})
            .filter(GL.filter(t -> t.getType() == 0 && t.getSpeed() == 0))
            .keyBy(GL.key(t -> t.getKey()), TypeInformation.of(String.class))
            .window(
                SlidingEventTimeWindows.of(
                    STOPPED_VEHICLE_WINDOW_SIZE, STOPPED_VEHICLE_WINDOW_SLIDE))
            .aggregate(
                GL.aggregate(
                    new LinearRoadVehicleAggregate()));
      sourceStream.print();

      /*
    sourceStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<ProvenanceTupleContainer<VehicleTuple>>() {
        GenealogGraphTraverser ggt = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
        @Override
        public ProducerRecord<byte[], byte[]> serialize(ProvenanceTupleContainer<VehicleTuple> tuple, @Nullable Long aLong) {
            // String ret = "{\"OUT\":\"" + tuple.toString() + ",\"STIM\":\"" + tuple.getStimulus() + "\"}";
            String lineage = FormatLineage.formattedLineage(ggt.getProvenance(tuple));
            String ret = "{\"OUT\":\"" + tuple.tuple() + "\",\"LINEAGE\":[" + lineage + "]" + ",\"FLAG\":\"" + tuple.getStimulus() + "\"}";
            return new ProducerRecord<>(outputTopicName, ret.getBytes(StandardCharsets.UTF_8));
        }
    }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
       */

    env.execute("LinearRoadCombined");
  }
}
