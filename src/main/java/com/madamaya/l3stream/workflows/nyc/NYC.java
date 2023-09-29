package com.madamaya.l3stream.workflows.nyc;

import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.ops.CountAndAvgDistance;
import com.madamaya.l3stream.workflows.nyc.ops.DataParserNYC;
import com.madamaya.l3stream.workflows.nyc.ops.WatermarkStrategyNYC;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class NYC {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "NYC";
        final String inputTopicName = queryFlag + "-i";
        final String outputTopicName = queryFlag + "-o";

        boolean local = true;
        Properties kafkaProperties = new Properties();
        if (local) {
            kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        } else {
            kafkaProperties.setProperty("bootstrap.servers", "172.16.0.209:9092,172.16.0.220:9092");
        }
        kafkaProperties.setProperty("group.id", "myGROUP");
        kafkaProperties.setProperty("transaction.timeout.ms", "540000");

        /* Query */
        env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest())
                .map(new DataParserNYC())
                .assignTimestampsAndWatermarks(new WatermarkStrategyNYC())
                .filter(t -> t.getTripDistance() > 5)
                .keyBy(t -> Tuple2.of(t.getVendorId(), t.getDropoffLocationId()))
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(new CountAndAvgDistance())
                .addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<NYCResultTuple>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(NYCResultTuple tuple, @Nullable Long aLong) {
                        return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    }
}
