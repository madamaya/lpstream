package com.madamaya.l3stream.workflows.unused.joinTest;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.l3stream.util.deserializerV2.StringDeserializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class JoinTest {
    public static void main(String[] args) throws Exception {
        /* Define variables & Create environment */
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.getConfig().enableObjectReuse();
        env.setParallelism(1);

        final String queryFlag = "Jointest";
        final String inputTopicName = queryFlag + "-i";
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
        DataStream<KafkaInputString> sourceDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSourceNexmark");
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> ds1 = sourceDs
                .map(new MapFunction<KafkaInputString, Tuple4<Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple4<Integer, Integer, Integer, Integer> map(KafkaInputString kafkaInputString) throws Exception {
                        String[] elements = kafkaInputString.getStr().split(",");
                        return Tuple4.of(Integer.valueOf(elements[0]), Integer.valueOf(elements[1]), Integer.valueOf(elements[2]), Integer.valueOf(elements[3]));
                    }
                })
                .filter(t -> t.f0 == 0)
                .assignTimestampsAndWatermarks(new WatermarkStrategy3());

        DataStream<Tuple4<Integer, Integer, Integer, Integer>> ds2 = sourceDs
                .map(new MapFunction<KafkaInputString, Tuple4<Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple4<Integer, Integer, Integer, Integer> map(KafkaInputString kafkaInputString) throws Exception {
                        String[] elements = kafkaInputString.getStr().split(",");
                        return Tuple4.of(Integer.valueOf(elements[0]), Integer.valueOf(elements[1]), Integer.valueOf(elements[2]), Integer.valueOf(elements[3]));
                    }
                })
                .filter(t -> t.f0 == 1)
                .assignTimestampsAndWatermarks(new WatermarkStrategy3());

        DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out = ds1.keyBy(t -> t.f1)
                .intervalJoin(ds2.keyBy(t -> t.f1))
                .between(Time.milliseconds(0), Time.milliseconds(1000))
                .process(new ProcessJoinFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple4<Integer, Integer, Integer, Integer> t1, Tuple4<Integer, Integer, Integer, Integer> t2, ProcessJoinFunction<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>.Context context, Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {
                        collector.collect(Tuple8.of(t1.f0, t1.f1, t1.f2, t1.f3, t2.f0, t2.f1, t2.f2, t2.f3));
                    }
                });

        out.print();

        env.execute("JoinTest.java");
    }

}
