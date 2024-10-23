package com.madamaya.l3stream.workflows.nyc;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.ops.CountAndAvgDistanceL3;
import com.madamaya.l3stream.workflows.nyc.ops.DataParserNYCL3;
import com.madamaya.l3stream.workflows.nyc.ops.TsAssignNYCL3;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.LineageModeStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.NonLineageModeStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class L3NYC {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "NYC";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = settings.getOutputTopicName(queryFlag + "-o");
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(settings.setOffsetsInitializer())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        // Source & InitMap & Parse
        DataStream<L3StreamTupleContainer<NYCInputTuple>> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNYC").uid("1")
                .map(L3.initMap(settings)).uid("2")
                .map(L3.map(new DataParserNYCL3())).uid("3");
        // Additional operator (assignChkTs or extractInputTs)
        DataStream<L3StreamTupleContainer<NYCInputTuple>> ds2;
        if (L3.getClass() == NonLineageModeStrategy.class) {
            ds2 = ds.map(L3.assignChkTs(new WatermarkStrategyNYC(), 0)).uid("4");
        } else {
            ds2 = ds.map(L3.extractInputTs(new WatermarkStrategyNYC())).uid("5");
        }
        // Main process
        DataStream<L3StreamTupleContainer<NYCResultTuple>> ds3 = ds2
                .assignTimestampsAndWatermarks(L3.assignTimestampsAndWatermarks(new WatermarkStrategyNYC(), settings.readPartitionNum(env.getParallelism()))).uid("6")
                .filter(L3.filter(t -> t.getTripDistance() > 5)).uid("7")
                .map(L3.mapTs(new TsAssignNYCL3())).uid("8")
                .keyBy(L3.keyBy(new KeySelector<NYCInputTuple, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> getKey(NYCInputTuple tuple) throws Exception {
                        return Tuple2.of(tuple.getVendorId(), tuple.getDropoffLocationId());
                    }
                }), TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(Integer.class, Long.class))
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(L3.aggregateTs(new CountAndAvgDistanceL3())).uid("9");

        // Sink
        Properties props = new Properties();
        if (L3.getClass() == LineageModeStrategy.class) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485880);
        }
        ds3.process(L3.extractTs()).uid("10").sinkTo(settings.getKafkaSink().newInstance(outputTopicName, brokers, settings, props)).uid(settings.getLineageMode());

        env.execute(settings.getLineageMode() + ": " + queryFlag);
    }
}
