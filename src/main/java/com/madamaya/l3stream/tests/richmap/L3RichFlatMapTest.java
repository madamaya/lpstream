package com.madamaya.l3stream.tests.richmap;

import com.madamaya.l3stream.l3operator.util.CpAssigner;
import io.palyvos.provenance.l3stream.cpm.CpManagerClient;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class L3RichFlatMapTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        String inputTopicName = "rfmap";
        String outputTopicName = "rfmapout";

        env.setParallelism(1);

        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        final L3OpWrapperStrategy L3 = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
        FlinkSerializerActivator.L3STREAM.activate(env, settings);

        DataStream<L3StreamTupleContainer<String>> ds = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromLatest()).uid("source")
                .map(L3.initMap(t->System.nanoTime(), t->System.nanoTime(), settings, "")).uid("initmap")
                .map(L3.map(new DataParser())).uid("parser")
                .flatMap(L3.flatMap(new MyRichFlatMap())).uid("rfmap");

        if (settings.getLineageMode() == "NonLineageMode") {
            ds.map(new CpAssigner<>()).uid("cpassigner-non")
                    .addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("sink-non");
        } else {
            env.getCheckpointConfig().disableCheckpointing();
            ds.map(new CpAssigner<>()).uid("cpassigner-lin")
                    .addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings)).uid("sink-lin");
        }

        if (settings.cpmProcessing()) {
            DataStream<ObjectNode> ds2 = env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).uid("subp").setParallelism(1)
                    .map(new CpManagerClient()).uid("cpmanager").setParallelism(1);
        }

        env.execute("output");
    }

    public static class DataParser implements MapFunction<ObjectNode, String> {

        @Override
        public String map(ObjectNode jsonNodes) throws Exception {
            return jsonNodes.get("value").textValue();
        }
    }

    public static class MyRichFlatMap extends RichFlatMapFunction<String, String> {
        BufferedWriter bw;
        int count;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            count = 0;
            try {
                bw = new BufferedWriter(new FileWriter("/Users/yamada-aist/workspace/l3stream/data/test0918/" + System.currentTimeMillis() + ".log"));
            } catch (Exception e) {
                System.err.println(e);
            }
        }

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String out = s;
            count++;
            bw.write(count + "\n");
            bw.flush();
            collector.collect(out);
        }
    }

}
