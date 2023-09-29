package com.madamaya.l3stream.workflows.ysb;

import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import com.madamaya.l3stream.workflows.ysb.ops.*;
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

public class YSB {
    public static void main(String[] args) throws Exception {

        /* Define variables & Create environment */
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        final String queryFlag = "YSB";
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
                .map(new DataParserYSB())
                .assignTimestampsAndWatermarks(new WatermarkStrategyYSB())
                .filter(t -> t.getAdType() == "view")
                .map(new ProjectAttributeYSB())
                .keyBy(t -> t.getCampaignId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // .trigger(new TriggerYSB())
                .aggregate(new CountYSB())
                .addSink(new FlinkKafkaProducer<>(outputTopicName, new KafkaSerializationSchema<YSBResultTuple>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(YSBResultTuple tuple, @Nullable Long aLong) {
                        return new ProducerRecord<>(outputTopicName, tuple.toString().getBytes(StandardCharsets.UTF_8));
                    }
                }, kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    }
}
