package com.madamaya.l3stream.workflows.unused.nyc2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.ops.CountAndAvgDistanceL3;
import com.madamaya.l3stream.workflows.nyc.ops.DataParserNYCL3;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class L3NYC2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();
        if (settings.getLineageMode() == "LineageMode") {
            // env.getCheckpointConfig().disableCheckpointing();
        }

        final String queryFlag = "NYC2";
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
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        //DataStream<L3StreamTupleContainer<NYCResultTuple>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
        DataStream<L3StreamTupleContainer<NYCResultTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNYC").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserNYCL3())).uid("3")
                .map(L3.updateTsWM(new WatermarkStrategyNYC(), 0)).uid("4")
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyNYC(), settings.readPartitionNum(env.getParallelism()))).uid("5")
                .filter(L3.filter(t -> t.getTripDistance() > 5)).uid("6")
                /*
                .keyBy(L3.keyBy(new KeySelector<NYCInputTuple, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTuple tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                }), TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(Integer.class, Long.class))
                 */
                .keyBy(L3.keyBy(new KeySelector<NYCInputTuple, Integer>() {
                    int key = 0;
                    @Override
                    public Integer getKey(NYCInputTuple tuple) throws Exception {
                        key = (key + 1) % 20;
                        return key;
                    }
                }, Integer.class))
                .window(TumblingEventTimeWindows.of(settings.assignExperimentWindowSize(Time.seconds(30))))
                .aggregate(L3.aggregate(new CountAndAvgDistanceL3())).uid("7");

        // L5
        Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        if (settings.isInvokeCpAssigner()) {
            ds.map(new CpAssigner<>()).uid("8").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode());
        } else {
            ds.sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode());
        }
        /*
        if (settings.getLineageMode() == "NonLineageMode") {
            // ds.map(new CpAssigner<>()).uid("8").addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("9");
            ds.map(new CpAssigner<>()).uid("8").sinkTo(NonLineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("9");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            // ds.map(new CpAssigner<>()).uid("10").addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("11");
            ds.map(new CpAssigner<>()).uid("10").sinkTo(LineageKafkaSinkV2.newInstance(outputTopicName, brokers, settings)).uid("11");
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
