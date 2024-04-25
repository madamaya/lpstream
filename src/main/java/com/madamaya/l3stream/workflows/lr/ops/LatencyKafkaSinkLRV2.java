package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkLRV2 implements KafkaRecordSerializationSchema<LinearRoadInputTuple> {
    private String topic;

    public LatencyKafkaSinkLRV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (latency + "," + tuple.getKafkaAppendTime() + "," + tuple.getDominantOpTime() + "," + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
