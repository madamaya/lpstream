package com.madamaya.l3stream.workflows.nexmark2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Nexmark2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        /*
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
         */

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("1" + String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        DataStream<KafkaInputString> sourceDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark");
        DataStream<NexmarkAuctionTuple> auction = sourceDs
                .map(new AuctionDataParserNex(settings))
                .filter(t -> t.getEventType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNex());

        DataStream<NexmarkBidTuple> bid = sourceDs
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
                .between(Time.milliseconds(0), settings.assignExperimentWindowSize(Time.milliseconds(20)))
                .process(new JoinNex())
                .filter(t -> t.getCategory() == 10);

        KafkaSink<NexmarkJoinedTuple> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NexmarkJoinedTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new OutputKafkaSinkNexmarkV2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NexmarkJoinedTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkNexmarkV2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
