package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkSyn1V2 implements KafkaRecordSerializationSchema<SynTempTuple> {
    private String topic;

    public LatencyKafkaSinkSyn1V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynTempTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}