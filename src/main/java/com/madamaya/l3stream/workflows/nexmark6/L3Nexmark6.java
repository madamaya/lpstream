package com.madamaya.l3stream.workflows.nexmark6;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.ops.AuctionDataParserNexL3;
import com.madamaya.l3stream.workflows.nexmark.ops.BidderDataParserNexL3;
import com.madamaya.l3stream.workflows.nexmark.ops.WatermarkStrategyAuctionNex;
import com.madamaya.l3stream.workflows.nexmark.ops.WatermarkStrategyBidNex;
import com.madamaya.l3stream.workflows.nexmark4.ops.JoinNexL34;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class L3Nexmark6 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();
        if (settings.getLineageMode() == "LineageMode") {
            env.getCheckpointConfig().disableCheckpointing();
        }

        final String queryFlag = "Nexmark";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
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

        KafkaSource<KafkaInputString> source2 = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId("2" + String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        // DataStream<KafkaInputString> sourceDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark").uid("1");
        // DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("1")
        DataStream<L3StreamTupleContainer<NexmarkAuctionTuple>> auction = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark1").uid("1")
                .map(L3.initMap(settings, 0)).uid("2")
                .map(L3.map(new AuctionDataParserNexL3())).uid("3")
                .filter(L3.filter(t -> t.getEventType() == 1)).uid("4")
                .map(L3.updateTsWM(new WatermarkStrategyAuctionNex(), 0)).uid("5")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyAuctionNex(), settings.readPartitionNum(env.getParallelism()))).uid("6");

        // DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest()).uid("6")
        DataStream<L3StreamTupleContainer<NexmarkBidTuple>> bid = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark2").uid("7")
                .map(L3.initMap(settings, 1)).uid("8")
                .map(L3.map(new BidderDataParserNexL3())).uid("9")
                .filter(L3.filter(t -> t.getEventType() == 2)).uid("10")
                .map(L3.updateTsWM(new WatermarkStrategyBidNex(), 1)).uid("11")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyBidNex(), settings.readPartitionNum(env.getParallelism()))).uid("12");

        /*
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
                .between(Time.milliseconds(0), settings.assignExperimentWindowSize(Time.milliseconds(20)))
                .process(L3.processJoin(new JoinNexL3())).uid("13")
                .filter(L3.filter(t -> t.getCategory() == 10)).uid("14");
         */
        DataStream<L3StreamTupleContainer<NexmarkJoinedTuple>> joined = auction.join(bid)
                .where(L3.keyBy(t -> t.getAuctionId(), Integer.class))
                .equalTo(L3.keyBy(t -> t.getAuctionId(), Integer.class))
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply(L3.join(new JoinNexL34()))
                .filter(L3.filter(t -> t.getCategory() == 10)).uid("14");

        // L5
        if (settings.isInvokeCpAssigner()) {
            joined.map(new CpAssigner<>()).uid("15").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        } else {
            joined.sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings)).uid(settings.getLineageMode());
        }
        /*
            if (settings.getLineageMode() == "NonLineageMode") {
                if (settings.isInvokeCpAssigner()) {
                    joined.map(new CpAssigner<>()).uid("13").sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("14");
                } else {
                    joined.sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("15");
                }
            } else {
                if (settings.isInvokeCpAssigner()) {
                    joined.map(new CpAssigner<>()).uid("16").sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("17");
                } else {
                    joined.sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("18");
                }
            }
          */

        /*
        if (settings.cpmProcessing()) {
            KafkaSource<L3StreamInput<String>> tempSource = KafkaSource.<L3StreamInput<String>>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(inputTopicName)
                    .setGroupId("tempSource")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setDeserializer(new StringL3DeserializerV2())
                    .build();

            DataStream ds2 = env.fromSource(tempSource, WatermarkStrategy.noWatermarks(), "tempSource").uid("100").setParallelism(1)
                    .map(new CpManagerClient()).uid("101").setParallelism(1);
        }
         */

        env.execute(settings.getLineageMode() + "," + queryFlag);
    }
}
