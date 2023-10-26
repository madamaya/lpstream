package com.madamaya.l3stream.workflows.lr;

import com.madamaya.l3stream.workflows.lr.ops.DataParserLR;
import com.madamaya.l3stream.workflows.lr.ops.WatermarkStrategyLR;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
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

public class LR {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        DataStream<CountTuple> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new DataParserLR(settings))
                .assignTimestampsAndWatermarks(new WatermarkStrategyLR())
                .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
                .keyBy(t -> t.getKey())
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(STOPPED_VEHICLE_WINDOW_SIZE),
                        STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(new LinearRoadVehicleAggregate())
                .filter(t -> t.getReports() == 4 && t.isUniquePosition())
                .keyBy(t -> t.getLatestPos())
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(ACCIDENT_WINDOW_SIZE),
                        ACCIDENT_WINDOW_SLIDE))
                .aggregate(new LinearRoadAccidentAggregate())
                //.slotSharingGroup(settings.secondSlotSharingGroup())
                .filter(t -> t.getCount() > 1);

        if (settings.getLatencyFlag() == 1) {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<CountTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(CountTuple tuple, @Nullable Long aLong) {
                    return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    // return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        } else {
            ds.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<CountTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(CountTuple tuple, @Nullable Long aLong) {
                    // return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }

        env.execute("Query: " + queryFlag);
    }
}
