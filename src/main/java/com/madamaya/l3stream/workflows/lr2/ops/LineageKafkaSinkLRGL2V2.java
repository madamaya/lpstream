package com.madamaya.l3stream.workflows.lr2.ops;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.usecases.CountTupleGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class LineageKafkaSinkLRGL2V2 implements KafkaRecordSerializationSchema<LinearRoadInputTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LineageKafkaSinkLRGL2V2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        String lineage = FormatLineage.formattedLineage(genealogGraphTraverser.getProvenance(tuple));
        String ret = "{\"OUT\":\"" + tuple + "\",\"TS\":\"" + tuple.getTimestamp() + "\",\"LINEAGE\":[" + lineage + "]}";
        return new ProducerRecord<>(topic, ret.getBytes(StandardCharsets.UTF_8));
    }
}
