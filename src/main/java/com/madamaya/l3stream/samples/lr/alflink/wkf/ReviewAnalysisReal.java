package com.madamaya.l3stream.samples.lr.alflink.wkf;

import com.madamaya.l3stream.samples.lr.alflink.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class ReviewAnalysisReal {

  public static void main(String[] args) throws Exception {
    boolean local = false;

    Params params;
    if (local) {
        params = new Params(0, "test", 1, 1, 1000);
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

    DataStream<PredictedData> resultStream = env
            .addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
            // .map(new ConvertJsonDataReal())
            .map(new ConvertJsonDataRealTh2(params))
            .filter(new FilterFunction<ReviewInputData>() {
                @Override
                public boolean filter(ReviewInputData value) throws Exception {
                    return value.validate();
                }
            })
            .map(new ReasonGeneableOperator(params));
    resultStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new MyKafkaSerializerReal(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    env.execute("WKF real: " + params.getCommentLen());
  }
}
