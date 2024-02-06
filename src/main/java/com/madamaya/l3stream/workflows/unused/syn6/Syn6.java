package com.madamaya.l3stream.workflows.unused.syn6;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import com.madamaya.l3stream.workflows.syn1.ops.*;
import com.madamaya.l3stream.workflows.syn1.ops.TsAssignPowerMap;
import com.madamaya.l3stream.workflows.syn1.ops.TsAssignTempMap;
import com.madamaya.l3stream.workflows.unused.syn6.ops.JoinSyn6;
import com.madamaya.l3stream.workflows.unused.syn6.ops.LatencyKafkaSinkSyn6V2;
import com.madamaya.l3stream.workflows.unused.syn6.ops.OutputKafkaSinkSyn6V2;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Syn6 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn6";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputString> source = KafkaSource.<KafkaInputString>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2())
                .build();

        /* Query */
        DataStream<KafkaInputString> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn2");
        DataStream<SynTempTuple> temp = ds
                .map(new TempParserSyn(settings))
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSyn())
                .map(new TsAssignTempMap());

        DataStream<SynPowerTuple> power = ds
                .map(new PowerParserSyn(settings))
                .filter(t -> t.getType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyPowerSyn())
                .map(new TsAssignPowerMap());

        DataStream<SynJoinedTuple> joined = power.join(temp)
                .where(new KeySelector<SynPowerTuple, Integer>() {
                    @Override
                    public Integer getKey(SynPowerTuple synPowerTuple) throws Exception {
                        return synPowerTuple.getMachineId();
                    }
                }).equalTo(new KeySelector<SynTempTuple, Integer>() {
                            @Override
                            public Integer getKey(SynTempTuple synTempTuple) throws Exception {
                                return synTempTuple.getMachineId();
                            }
                }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new JoinSyn6());

        KafkaSink<SynJoinedTuple> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynJoinedTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new OutputKafkaSinkSyn6V2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynJoinedTuple>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkSyn6V2(outputTopicName))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
