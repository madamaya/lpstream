package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LatencyKafkaSinkNexmarkV2 implements KafkaRecordSerializationSchema<NexmarkJoinedTuple> {
    private String topic;

    public LatencyKafkaSinkNexmarkV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(NexmarkJoinedTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, latency.getBytes(StandardCharsets.UTF_8));
    }
}