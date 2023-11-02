package com.madamaya.l3stream.workflows.nexmark;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdata;
import com.madamaya.l3stream.glCommons.InitGdataJsonNodeGL;
import com.madamaya.l3stream.workflows.lr.ops.LatencyKafkaSinkLRGLV2;
import com.madamaya.l3stream.workflows.lr.ops.LineageKafkaSinkLRGLV2;
import com.madamaya.l3stream.workflows.nexmark.objects.*;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.JsonNodeL3DeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.usecases.CountTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class GLNexmark {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        Properties kafkaProperties = new Properties();
        Properties kafkaProperties2 = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties2.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", "1" + String.valueOf(System.currentTimeMillis()));
        kafkaProperties2.setProperty("group.id", "2" + String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        kafkaProperties2.setProperty("transaction.timeout.ms", "540000");

        KafkaSource<L3StreamInput<JsonNode>> source = KafkaSource.<L3StreamInput<JsonNode>>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("myGroup")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new JsonNodeL3DeserializerV2())
                .build();

        /* Query */
        // DataStream<NexmarkAuctionTupleGL> auction = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<NexmarkAuctionTupleGL> auction = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark")
                .map(new InitGLdata<>(settings, 0))
                .map(new AuctionDataParserNexGL())
                .filter(t -> t.getEventType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNexGL());

        // DataStream<NexmarkBidTupleGL> bid = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<NexmarkBidTupleGL> bid = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark")
                .map(new InitGLdata<>(settings, 1))
                .map(new BidderDataParserNexGL())
                .filter(t -> t.getEventType() == 2)
                .assignTimestampsAndWatermarks(new WatermarkStrategyBidNexGL());

        DataStream<NexmarkJoinedTupleGL> joined = auction.keyBy(new KeySelector<NexmarkAuctionTupleGL, Integer>() {
                @Override
                public Integer getKey(NexmarkAuctionTupleGL tuple) throws Exception {
                    return tuple.getAuctionId();
                }
                })
                .intervalJoin(bid.keyBy(new KeySelector<NexmarkBidTupleGL, Integer>() {
                    @Override
                    public Integer getKey(NexmarkBidTupleGL tuple) throws Exception {
                        return tuple.getAuctionId();
                    }
                }))
                .between(Time.milliseconds(0), settings.assignExperimentWindowSize(Time.milliseconds(20)))
                .process(new JoinNexGL())
                .filter(t -> t.getCategory() == 10);

        KafkaSink<NexmarkJoinedTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NexmarkJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkNexmarkGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NexmarkJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkNexmarkGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
