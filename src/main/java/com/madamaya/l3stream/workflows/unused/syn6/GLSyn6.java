package com.madamaya.l3stream.workflows.unused.syn6;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.glCommons.InitGLdataStringGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import com.madamaya.l3stream.workflows.syn1.ops.*;
import com.madamaya.l3stream.workflows.unused.syn4.ops.TsAssignPowerMapGL;
import com.madamaya.l3stream.workflows.unused.syn4.ops.TsAssignTempMapGL;
import com.madamaya.l3stream.workflows.unused.syn6.ops.JoinSynGL6;
import com.madamaya.l3stream.workflows.unused.syn6.ops.LatencyKafkaSinkSyn6GLV2;
import com.madamaya.l3stream.workflows.unused.syn6.ops.LineageKafkaSinkSyn6GLV2;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
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

public class GLSyn6 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
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
        DataStream<SynTempTupleGL> temp = ds
                .map(new InitGLdataStringGL(settings, 0))
                .map(new TempParserSynGL())
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSynGL())
                .map(new TsAssignTempMapGL());

        DataStream<SynPowerTupleGL> power = ds
                .map(new InitGLdataStringGL(settings, 1))
                .map(new PowerParserSynGL())
                .filter(t -> t.getType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyPowerSynGL())
                .map(new TsAssignPowerMapGL());

        DataStream<SynJoinedTupleGL> joined = power.join(temp)
                .where(new KeySelector<SynPowerTupleGL, Integer>() {
            @Override
            public Integer getKey(SynPowerTupleGL synPowerTuple) throws Exception {
                return synPowerTuple.getMachineId();
            }
        })
        .equalTo(new KeySelector<SynTempTupleGL, Integer>() {
            @Override
            public Integer getKey(SynTempTupleGL synTempTuple) throws Exception {
                return synTempTuple.getMachineId();
            }
        })
        .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .apply(new JoinSynGL6());

        KafkaSink<SynJoinedTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkSyn6GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkSyn6GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}