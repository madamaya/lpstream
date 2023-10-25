package com.madamaya.l3stream.samples.lr.alflink.wwkfcp;

import com.madamaya.l3stream.l3operator.util.CpAssigner;
import com.madamaya.l3stream.samples.lr.alflink.*;
import io.palyvos.provenance.l3stream.cpm.CpManagerClient;
import io.palyvos.provenance.l3stream.util.LineageKafkaSink;
import io.palyvos.provenance.l3stream.util.NonLineageKafkaSink;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.L3OpWrapperStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.FlinkSerializerActivator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class ReviewAnalysisReal {

  public static void main(String[] args) throws Exception {
    boolean local = false;

    Params params;
    if (local) {
        params = new Params(0, "test", 1, 100, 1000);
    } else {
        params = Params.newInstance(System.getenv("EXP_CONF_FILE"));
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    env.getConfig().enableObjectReuse();

    Properties kafkaProperties = new Properties();
    String inputTopicName;
    String outputTopicName;
    if (local) {
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        inputTopicName = "input";
        outputTopicName = "output-r";
    } else {
        kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        inputTopicName = "input-syn" + params.getCommentLen();
        outputTopicName = "review-syn-real";
    }

    // L1: おまじない
    ExperimentSettings settings = ExperimentSettings.newInstance(args);
    final L3OpWrapperStrategy L3S = settings.l3OpWrapperStrategy().apply(settings.aggregateStrategySupplier());
    FlinkSerializerActivator.PROVENANCE_TRANSPARENT.activate(env, settings);
    // L1

      // L2
    DataStream<L3StreamTupleContainer<PredictedData>> resultStream = env
            .addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(true), kafkaProperties).setStartFromEarliest())
            // L4
            //.map(L3S.initMap(t->System.currentTimeMillis(), t->System.nanoTime(), settings, "real_"))
            //.map(L3S.initMap(t->System.nanoTime(), t->System.nanoTime(), settings, "real_"))
            .map(L3S.initMap(settings))
            // L3
            .map(L3S.map(new ConvertJsonDataReal()))
            .filter(L3S.filter(new FilterFunction<ReviewInputData>() {
                @Override
                public boolean filter(ReviewInputData value) throws Exception {
                    return value.validate();
                }
            }))
            .map(L3S.map(new ReasonGeneableOperator(params)));


      DataStream<ObjectNode> ds2 =  env.addSource(new FlinkKafkaConsumer<>("temp", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest()).setParallelism(1)
              .map(new CpManagerClient()).setParallelism(1);

    // L5
    if (settings.getLineageMode() == "NonLineageMode") {
        resultStream.map(new CpAssigner<>()).addSink(NonLineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings));
    } else {
        resultStream.addSink(LineageKafkaSink.newInstance(outputTopicName, kafkaProperties, settings));
    }

    env.execute("WWKF real: " + params.getCommentLen());
  }
}
