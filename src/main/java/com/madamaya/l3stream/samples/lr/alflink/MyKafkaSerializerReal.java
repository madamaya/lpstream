package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class MyKafkaSerializerReal implements KafkaSerializationSchema<PredictedData> {

    private String outputTopic;

    public MyKafkaSerializerReal(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(PredictedData element, @Nullable Long timestamp) {
        long currentTime = System.nanoTime();

        String latency = Long.toString(currentTime - element.getStimulus());
        return new ProducerRecord<>(outputTopic, latency.getBytes(StandardCharsets.UTF_8));
    }
}
