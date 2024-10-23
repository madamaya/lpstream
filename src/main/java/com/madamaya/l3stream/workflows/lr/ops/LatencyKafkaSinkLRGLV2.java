package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class LatencyKafkaSinkLRGLV2 implements KafkaRecordSerializationSchema<LinearRoadInputTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LatencyKafkaSinkLRGLV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(LinearRoadInputTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        long traversalStartTime = System.nanoTime();
        Set<TimestampedUIDTuple> lineage = genealogGraphTraverser.getProvenance(tuple);
        long traversalEndTime = System.nanoTime();
        String lineageStr = FormatLineage.formattedLineage(lineage);

        String latency = Long.toString(traversalEndTime - tuple.getStimulus());
        String traversalTime = Long.toString(traversalEndTime - traversalStartTime);
        return new ProducerRecord<>(topic, (latency + "," + tuple.getKafkaAppendTime() + "," + tuple.getDominantOpTime() + "," + traversalTime + ",Lineage(" + lineage.size() + ")[" + lineageStr + "]," + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
