package com.madamaya.l3stream.workflows.syn10.ops;

import com.madamaya.l3stream.workflows.syn10.objects.SynTempTestTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkTestV2 implements KafkaRecordSerializationSchema<SynTempTestTuple> {
    private String topic;

    public OutputKafkaSinkTestV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynTempTestTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
