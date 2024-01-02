package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkSyn1V2 implements KafkaRecordSerializationSchema<SynTempTuple> {
    private String topic;

    public OutputKafkaSinkSyn1V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynTempTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
