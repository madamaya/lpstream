package com.madamaya.l3stream.workflows.lr;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdata;
import com.madamaya.l3stream.glCommons.InitGdataJsonNodeGL;
import com.madamaya.l3stream.glCommons.InitGdataStringGL;
import com.madamaya.l3stream.workflows.lr.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringGLDeserializerV2;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringL3DeserializerV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLinV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.CountTupleGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadAccidentAggregate;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadVehicleAggregate;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

import static io.palyvos.provenance.usecases.linearroad.LinearRoadConstants.*;

public class GLLR {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "LR";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
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
                .setDeserializer(new StringGLDeserializerV2())
                .build();

        /* Query */
        //DataStream<CountTupleGL> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<CountTupleGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceLR")
                .map(new InitGLdata<>(settings))
                .map(new DataParserLRGL())
                .assignTimestampsAndWatermarks(new WatermarkStrategyLRGL())
                .filter(t -> t.getType() == 0 && t.getSpeed() == 0)
                .keyBy(t -> t.getKey())
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(STOPPED_VEHICLE_WINDOW_SIZE),
                        STOPPED_VEHICLE_WINDOW_SLIDE))
                .aggregate(new LinearRoadVehicleAggregate(settings.aggregateStrategySupplier()))
                .filter(t -> t.getReports() == (4 * settings.getWindowSize()) && t.isUniquePosition())
                .keyBy(t -> t.getLatestPos())
                .window(SlidingEventTimeWindows.of(settings.assignExperimentWindowSize(ACCIDENT_WINDOW_SIZE),
                        ACCIDENT_WINDOW_SLIDE))
                .aggregate(new LinearRoadAccidentAggregate(settings.aggregateStrategySupplier()))
                //.slotSharingGroup(settings.secondSlotSharingGroup())
                .filter(t -> t.getCount() > 1);

        KafkaSink<CountTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<CountTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkLRGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<CountTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkLRGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        }
        ds.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
