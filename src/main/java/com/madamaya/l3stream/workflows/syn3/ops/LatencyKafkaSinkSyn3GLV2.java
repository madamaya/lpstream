package com.madamaya.l3stream.workflows.syn3.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class LatencyKafkaSinkSyn3GLV2 implements KafkaRecordSerializationSchema<SynResultTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LatencyKafkaSinkSyn3GLV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SynResultTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        long traverseStart = System.nanoTime();
        Set<TimestampedUIDTuple> lineage = genealogGraphTraverser.getProvenance(tuple);
        long traserseEnd = System.nanoTime();
        int lineageSize = lineage.size();
        String lineageStr = FormatLineage.formattedLineage(lineage);

        String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        String traversalLatency = Long.toString(traserseEnd - traverseStart);
        return new ProducerRecord<>(topic, (latency + "," + tuple.getStimulus() + "," + traversalLatency + ", Lineage(" + lineageSize + ")" + lineageStr + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
