package com.madamaya.l3stream.workflows.utils;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.utils.ops.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LatencyCalc {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        if (args.length != 2) {
            throw new IllegalArgumentException();
        }
        final String topicName = args[0] + "-o";
        final String filePath = args[1];
        final String brokers = L3Config.BOOTSTRAP_IP_PORT;

        /* Kafka source configurations */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topicName)
                .setGroupId(String.valueOf(System.currentTimeMillis()))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new Deserializer())
                .build();

        /* Workflow */
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "LatencyCalcSource")
                .map(new Parse())
                .assignTimestampsAndWatermarks(new WStrategy())
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new Aggregation())
                .addSink(new LatencySink(filePath));

        env.execute();
    }
}
