package com.madamaya.l3stream.workflows.syn2.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynJoinedTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class LatencyKafkaSinkSyn2GLV2 implements KafkaRecordSerializationSchema<SynJoinedTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LatencyKafkaSinkSyn2GLV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynJoinedTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        Set<TimestampedUIDTuple> lineage = genealogGraphTraverser.getProvenance(tuple);
        String lineageStr = FormatLineage.formattedLineage(lineage);

        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
