package com.madamaya.l3stream.workflows.nexmark;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTupleGL;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2GL;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class GLNexmark {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputStringGL> source = KafkaSource.<KafkaInputStringGL>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("1" + String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2GL())
                .build();

        /* Query */
        DataStream<KafkaInputStringGL> sourceDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark");
        DataStream<NexmarkAuctionTupleGL> auction = sourceDs
                .map(new AuctionDataParserNexGL(settings))
                .filter(t -> t.getEventType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNexGL())
                .map(new TsAssignAuctionNexGL());

        DataStream<NexmarkBidTupleGL> bid = sourceDs
                .map(new BidderDataParserNexGL(settings))
                .filter(t -> t.getEventType() == 2)
                .assignTimestampsAndWatermarks(new WatermarkStrategyBidNexGL())
                .map(new TsAssignBidderNexGL());

        DataStream<NexmarkJoinedTupleGL> joined = auction.join(bid)
                .where(new KeySelector<NexmarkAuctionTupleGL, Integer>() {
                    @Override
                    public Integer getKey(NexmarkAuctionTupleGL nexmarkAuctionTupleGL) throws Exception {
                        return nexmarkAuctionTupleGL.getAuctionId();
                    }
                })
                .equalTo(new KeySelector<NexmarkBidTupleGL, Integer>() {
                    @Override
                    public Integer getKey(NexmarkBidTupleGL nexmarkBidTupleGL) throws Exception {
                        return nexmarkBidTupleGL.getAuctionId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(20)))
                .with(new JoinNexGL())
                .filter(t -> t.getCategory() == 10);

        KafkaSink<NexmarkJoinedTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<NexmarkJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkNexmarkGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<NexmarkJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkNexmarkGLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: GL" + queryFlag);
    }
}
