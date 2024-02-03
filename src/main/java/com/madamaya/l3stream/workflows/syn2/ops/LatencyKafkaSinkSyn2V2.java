package com.madamaya.l3stream.workflows.syn2.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkSyn2V2 implements KafkaRecordSerializationSchema<SynJoinedTuple> {
    private String topic;

    public LatencyKafkaSinkSyn2V2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynJoinedTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (latency + "," + tuple.getKafkaAppendTime() + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
