package com.madamaya.l3stream.workflows.syn2.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkSyn2V2 implements KafkaRecordSerializationSchema<SynJoinedTuple> {
    private String topic;

    public OutputKafkaSinkSyn2V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynJoinedTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
