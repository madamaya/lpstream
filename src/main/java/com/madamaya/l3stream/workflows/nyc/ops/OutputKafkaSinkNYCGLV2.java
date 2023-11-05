package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkNYCGLV2 implements KafkaRecordSerializationSchema<NYCResultTuple> {
    private String topic;

    public OutputKafkaSinkNYCGLV2(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(NYCResultTuple tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, tuple.toString().getBytes(StandardCharsets.UTF_8));
    }
}