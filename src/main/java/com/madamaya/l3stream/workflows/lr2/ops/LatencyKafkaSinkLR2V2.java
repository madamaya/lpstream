package com.madamaya.l3stream.workflows.lr2.ops;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkLR2V2 implements KafkaRecordSerializationSchema<LinearRoadInputTuple> {
    private String topic;

    public LatencyKafkaSinkLR2V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getTfl().ts2);
        // return new ProducerRecord<>(topic, latency.getBytes(StandardCharsets.UTF_8));
        return new ProducerRecord<>(topic, (tuple.getTfl().ts1 + "," + latency + "," + tuple.getTfl() + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
