package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkNexmarkV2 implements KafkaRecordSerializationSchema<NexmarkJoinedTuple> {
    private String topic;

    public OutputKafkaSinkNexmarkV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(NexmarkJoinedTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
