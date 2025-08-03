package com.madamaya.l3stream.workflows.nexmark2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.ops.*;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class L3Nexmark2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Nexmark2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("1" + String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(settings.setOffsetsInitializer())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        // Source
        DataStream<KafkaInputString> sourceDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark").uid("1");
        // InitMap & Parse & Filter
        DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction = sourceDs
                .map(L3.initMap(settings, 0)).uid("2")
                .map(L3.map(new AuctionDataParserNexL3())).uid("3")
                .filter(L3.filter(t -> t.getEventType() == 1)).uid("4");
        // Additional operator (assignChkTs or extractInputTs)
        /*
        DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            auction2 = auction.map(L3.assignChkTs(new WatermarkStrategyAuctionNex(), 0)).uid("5");
        } else {
            auction2 = auction.map(L3.extractInputTs(new WatermarkStrategyAuctionNex())).uid("6");
        }
         */
        // Main process
        DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction3 = auction
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNex(), settings.readPartitionNum(env.getParallelism()))).uid("7")
                .map(L3.mapTs(new TsAssignAuctionNexL3())).uid("8");

        // InitMap & Parse & Filter
        DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid = sourceDs
                .map(L3.initMap(settings, 1)).uid("9")
                .map(L3.map(new BidderDataParserNexL3())).uid("10")
                .filter(L3.filter(t -> t.getEventType() == 2)).uid("11");
        // Additional operator (assignChkTs or extractInputTs)
        /*
        DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            bid2 = bid.map(L3.assignChkTs(new WatermarkStrategyBidNex(), 1)).uid("12");
        } else {
            bid2 = bid.map(L3.extractInputTs(new WatermarkStrategyBidNex())).uid("13");
        }
         */
        // Main process
        DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid3 = bid
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyBidNex(), settings.readPartitionNum(env.getParallelism()))).uid("14")
                .map(L3.mapTs(new TsAssignBidderNexL3())).uid("15");

        // Main process
        DataStream<L3StreamTupleContainer<NexmarkJoinedTuple>> joined = auction3.join(bid3)
                .where(L3.keyBy(new KeySelector<NexmarkAuctionTuple, Integer>() {
                    @Override
                    public Integer getKey(NexmarkAuctionTuple auctionTuple) throws Exception {
                        return auctionTuple.getAuctionId();
                    }
                }, Integer.class))
                .equalTo(L3.keyBy(new KeySelector<NexmarkBidTuple, Integer>() {
                    @Override
                    public Integer getKey(NexmarkBidTuple bidTuple) throws Exception {
                        return bidTuple.getAuctionId();
                    }
                }, Integer.class))
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .with(L3.joinTs(new JoinNexL3())).uid("16")
                .filter(L3.filter(t -> t.getCategory() == 10)).uid("17");

        // Sink
        joined.process(L3.extractTs()).uid("18").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());

        env.execute(settings.getLineageMode() + ": " + queryFlag);
    }
}
