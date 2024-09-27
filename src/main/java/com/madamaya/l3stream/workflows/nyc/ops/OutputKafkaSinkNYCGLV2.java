package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.util.FormatLineageForTestGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkNYCGLV2 implements KafkaRecordSerializationSchema<NYCResultTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public OutputKafkaSinkNYCGLV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(NYCResultTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String lineage = FormatLineageForTestGL.formattedLineage(genealogGraphTraverser.getProvenance(tuple));
        String ret = tuple + ":::::" + lineage;
        return new ProducerRecord<>(topic, ret.getBytes(StandardCharsets.UTF_8));
    }
}
