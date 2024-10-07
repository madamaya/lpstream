package com.madamaya.l3stream.workflows.syn10.ops;

import com.madamaya.l3stream.workflows.syn10.objects.SynTempTestTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineageForTestGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class OutputKafkaSinkTestV2GL implements KafkaRecordSerializationSchema<SynTempTestTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;


    public OutputKafkaSinkTestV2GL(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynTempTestTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String lineage = FormatLineageForTestGL.formattedLineage(genealogGraphTraverser.getProvenance(tuple));
        String ret = tuple + ":::::" + lineage;
        return new ProducerRecord<>(topic, ret.getBytes(StandardCharsets.UTF_8));
    }
}
