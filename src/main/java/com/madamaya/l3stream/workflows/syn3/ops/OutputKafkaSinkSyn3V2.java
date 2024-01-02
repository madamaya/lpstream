package com.madamaya.l3stream.workflows.syn3.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkSyn3V2 implements KafkaRecordSerializationSchema<SynResultTuple> {
    private String topic;

    public OutputKafkaSinkSyn3V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynResultTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}
