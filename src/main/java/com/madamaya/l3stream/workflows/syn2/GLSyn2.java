package com.madamaya.l3stream.workflows.syn2;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nyc.ops.OutputKafkaSinkNYCGLV2;
import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import com.madamaya.l3stream.workflows.syn1.ops.*;
import com.madamaya.l3stream.workflows.syn2.ops.LatencyKafkaSinkSyn2GLV2;
import com.madamaya.l3stream.workflows.syn2.ops.LineageKafkaSinkSyn2GLV2;
import com.madamaya.l3stream.workflows.syn2.ops.OutputKafkaSinkSyn2GLV2;
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

public class GLSyn2 {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkSerializerActivator.L3STREAM.activate(env, settings);
        env.getConfig().enableObjectReuse();

        final String queryFlag = "Syn2";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        KafkaSource<KafkaInputStringGL> source = KafkaSource.<KafkaInputStringGL>builder()
                .setBootstrapServers(brokers)
                .setTopics(inputTopicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new StringDeserializerV2GL())
                .build();

        /* Query */
        DataStream<KafkaInputStringGL> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceSyn2");
        DataStream<SynTempTupleGL> temp = ds
                .map(new TempParserSynGL(settings))
                .filter(t -> t.getType() == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategyTempSynGL())
                .map(new TsAssignTempMapGL());

        DataStream<SynPowerTupleGL> power = ds
                .map(new PowerParserSynGL(settings))
                .filter(t -> t.getType() == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategyPowerSynGL())
                .map(new TsAssignPowerMapGL());

        DataStream<SynJoinedTupleGL> joined = power.join(temp)
                .where(new KeySelector<SynPowerTupleGL, Integer>() {
                    @Override
                    public Integer getKey(SynPowerTupleGL synPowerTuple) throws Exception {
                        return synPowerTuple.getMachineId();
                    }
                }).equalTo(new KeySelector<SynTempTupleGL, Integer>() {
                    @Override
                    public Integer getKey(SynTempTupleGL synTempTuple) throws Exception {
                        return synTempTuple.getMachineId();
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .with(new JoinSynGL());

        KafkaSink<SynJoinedTupleGL> sink;
        if (settings.getLatencyFlag() == 1) {
            sink = KafkaSink.<SynJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LineageKafkaSinkSyn2GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 100) {
            sink = KafkaSink.<SynJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new OutputKafkaSinkSyn2GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            sink = KafkaSink.<SynJoinedTupleGL>builder()
                    .setBootstrapServers(brokers)
                    .setRecordSerializer(new LatencyKafkaSinkSyn2GLV2(outputTopicName, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }
        joined.sinkTo(sink);

        env.execute("Query: " + queryFlag);
    }
}
