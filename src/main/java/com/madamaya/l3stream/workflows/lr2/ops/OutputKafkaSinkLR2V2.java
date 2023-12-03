package com.madamaya.l3stream.workflows.lr2.ops;

import io.palyvos.provenance.usecases.CountTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkLR2V2 implements KafkaRecordSerializationSchema<LinearRoadInputTuple> {
    private String topic;

    public OutputKafkaSinkLR2V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
