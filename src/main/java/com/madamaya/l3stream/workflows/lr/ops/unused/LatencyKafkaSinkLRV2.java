package com.madamaya.l3stream.workflows.lr.ops.unused;

import io.palyvos.provenance.usecases.CountTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkLRV2 implements KafkaRecordSerializationSchema<CountTuple> {
    private String topic;

    public LatencyKafkaSinkLRV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(CountTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        // return new ProducerRecord<>(topic, latency.getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
