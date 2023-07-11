package com.madamaya.l3stream.samples.lr.alflink.wkf;

import com.madamaya.l3stream.samples.lr.alflink.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class ReviewAnalysisWin {

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();

    Properties kafkaProperties = new Properties();
    String inputTopicName;
    String outputTopicName;
    if (local) {
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        inputTopicName = "input";
        outputTopicName = "output-w";
    } else {
        kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");
        inputTopicName = "input-syn" + params.getCommentLen();
        outputTopicName = "review-syn-win";
    }

    DataStream<Result> resultStream = env
            .addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
            // .map(new ConvertJsonDataWin())
            .map(new ConvertJsonDataWinTh2(params))
            .filter(new FilterFunction<ReviewInputData>() {
                @Override
                public boolean filter(ReviewInputData value) throws Exception {
                    return value.validate();
                }
            })
            .assignTimestampsAndWatermarks(
                    new WatermarkGen().withTimestampAssigner((reviewInputData, l) -> reviewInputData.getReviewTime())
            )
            .map(new ReasonGeneableOperator(params))
            .keyBy(new KeySelector<PredictedData, Tuple2<String, Boolean>>() {
                @Override
                public Tuple2<String, Boolean> getKey(PredictedData value) throws Exception {
                    return Tuple2.of(value.getProductId(), value.isPositive());
                }
            })
            .window(TumblingEventTimeWindows.of(Time.milliseconds(params.getWindowSize())))
            .aggregate(new CountRecords());
    resultStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new MyKafkaSerializerWin(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    env.execute("WKF win: " + params.getCommentLen());
  }
}
