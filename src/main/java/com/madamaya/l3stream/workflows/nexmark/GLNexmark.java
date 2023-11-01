package com.madamaya.l3stream.workflows.nexmark;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGdataGL;
import com.madamaya.l3stream.workflows.nexmark.objects.*;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import com.madamaya.l3stream.workflows.nyc.ops.LineageKafkaSinkNYCGL;
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

public class GLNexmark {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        Properties kafkaProperties = new Properties();
        Properties kafkaProperties2 = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties2.setProperty("bootstrap.servers", L3Config.BOOTSTRAP_IP_PORT);
        kafkaProperties.setProperty("group.id", "1" + String.valueOf(System.currentTimeMillis()));
        kafkaProperties2.setProperty("group.id", "2" + String.valueOf(System.currentTimeMillis()));
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        kafkaProperties2.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        DataStream<NexmarkAuctionTupleGL> auction = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new InitGdataGL(settings, 0))
                .map(new AuctionDataParserNexGL())
                .filter(t -> t.getEventType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNexGL());

        DataStream<NexmarkBidTupleGL> bid = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
                .map(new InitGdataGL(settings, 1))
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
                .between(Time.milliseconds(0), settings.assignExperimentWindowSize(Time.milliseconds(10)))
                .process(new JoinNexGL())
                .filter(t -> t.getCategory() == 10);

        if (settings.getLatencyFlag() == 1) {
            joined.addSink(new FlinkKafkaProducer<>(outputTopicName, new LineageKafkaSinkNexmarkGL(outputTopicName, settings), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        } else {
            joined.addSink(new FlinkKafkaProducer<>(outputTopicName, new LatencyKafkaSinkNexmarkGL(outputTopicName, settings), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        }

        /*
        joined.addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NexmarkJoinedTupleGL>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(NexmarkJoinedTupleGL tuple, @Nullable Long aLong) {
                return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
            }
        }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
         */

        env.execute("Query: " + queryFlag);
    }
}
