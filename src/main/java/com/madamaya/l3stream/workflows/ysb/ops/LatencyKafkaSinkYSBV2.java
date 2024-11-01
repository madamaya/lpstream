package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkYSBV2 implements KafkaRecordSerializationSchema<YSBResultTuple> {
    private String topic;

    public LatencyKafkaSinkYSBV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(YSBResultTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (latency + "," + tuple.getKafkaAppendTime() + "," + tuple.getDominantOpTime() + "," + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
