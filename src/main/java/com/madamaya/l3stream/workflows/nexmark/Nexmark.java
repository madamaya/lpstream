package com.madamaya.l3stream.workflows.nexmark;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.java.functions.KeySelector;
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

public class Nexmark {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        DataStream<NexmarkAuctionTuple> auction = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new AuctionDataParserNex(settings))
                .filter(t -> t.getEventType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNex());

        DataStream<NexmarkBidTuple> bid = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new BidderDataParserNex(settings))
                .filter(t -> t.getEventType() == 2)
                .assignTimestampsAndWatermarks(new WatermarkStrategyBidNex());

        DataStream<NexmarkJoinedTuple> joined = auction.keyBy(new KeySelector<NexmarkAuctionTuple, Integer>() {
                @Override
                public Integer getKey(NexmarkAuctionTuple tuple) throws Exception {
                    return tuple.getAuctionId();
                }
                })
                .intervalJoin(bid.keyBy(new KeySelector<NexmarkBidTuple, Integer>() {
                    @Override
                    public Integer getKey(NexmarkBidTuple tuple) throws Exception {
                        return tuple.getAuctionId();
                    }
                }))
                .between(Time.milliseconds(0), Time.milliseconds(20))
                .process(new JoinNex())
                .filter(t -> t.getCategory() == 10);

        if (settings.getLatencyFlag() == 1) {
            joined.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NexmarkJoinedTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(NexmarkJoinedTuple tuple, @Nullable Long aLong) {
                    return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    // return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        } else {
            joined.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NexmarkJoinedTuple>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(NexmarkJoinedTuple tuple, @Nullable Long aLong) {
                    // return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    return new ProducerRecord<>(outputTopicName, String.valueOf(System.nanoTime() - tuple.getStimulus()).getBytes(StandardCharsets.UTF_8));
                }
            }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }

        env.execute("Query: " + queryFlag);
    }
}
