package com.madamaya.l3stream.workflows.nexmark;

import com.madamaya.l3stream.cpstore.CpManagerClient;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class L3Nexmark {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        boolean local = true;
        Properties kafkaProperties = new Properties();
        Properties kafkaProperties2 = new Properties();
        if (local) {
            kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
            kafkaProperties2.setProperty("bootstrap.servers", "localhost:9092");
        } else {
            kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
            kafkaProperties2.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        }
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties2.setProperty("group.id", "myGROUP2");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        kafkaProperties2.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        // CNFM: updateTsWMのredisでバグるので修正 → 修正した
        DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
                .map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings)).uid("2")
                .map(L3.map(new AuctionDataParserNex())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyAuctionNex(), settings.getRedisIp(), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNex(), settings.numOfInstanceWM())).uid("5");

        DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("6")
                .map(L3.initMap(t -> System.nanoTime(), t -> System.nanoTime(), settings)).uid("7")
                .map(L3.map(new BidderDataParserNex())).uid("8")
                .map(L3.updateTsWM(new WatermarkStrategyBidNex(), settings.getRedisIp(), 1)).uid("9")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyBidNex(), settings.numOfInstanceWM())).uid("10");

        // CNFM: JOINでバグるので修正→一応した
        DataStream<L3StreamTupleContainer<NexmarkJoinedTuple>> joined = auction.keyBy(L3.keyBy(new KeySelector<NexmarkAuctionTuple, Integer>() {
                @Override
                public Integer getKey(NexmarkAuctionTuple tuple) throws Exception {
                    return tuple.getAuctionId();
                }
                }, Integer.class))
                .intervalJoin(bid.keyBy(L3.keyBy(new KeySelector<NexmarkBidTuple, Integer>() {
                    @Override
                    public Integer getKey(NexmarkBidTuple tuple) throws Exception {
                        return tuple.getAuctionId();
                    }
                }, Integer.class)))
                .between(Time.milliseconds(0), Time.milliseconds(20))
                .process(L3.processJoin(new JoinNex())).uid("11")
                .filter(L3.filter(t -> t.getCategory() == 10)).uid("12");

        // L5
        if (settings.getLineageMode() == "NonLineageMode") {
            joined.map(new CpAssigner<>()).uid("13").addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("14");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            joined.map(new CpAssigner<>()).uid("15").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("16");
        }

        if (settings.cpmProcessing()) {
            DataStream<ObjectNode> ds2 = env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("100").setParallelism(1)
                    .map(new CpManagerClient(settings)).uid("101").setParallelism(1);
        }

        env.execute("Query: " + queryFlag);
    }
}
