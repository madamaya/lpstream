package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTupleGL;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.util.FormatLineage;
import io.palyvos.provenance.l3stream.util.FormatStimulusList;
import io.palyvos.provenance.util.ExperimentSettings;
import io.palyvos.provenance.util.TimestampedUIDTuple;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class LatencyKafkaSinkYSBGLV2 implements KafkaRecordSerializationSchema<YSBResultTupleGL> {
    private String topic;
    private GenealogGraphTraverser genealogGraphTraverser;

    public LatencyKafkaSinkYSBGLV2(String topic, ExperimentSettings settings) {
        this.topic = topic;
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(YSBResultTupleGL tuple, KafkaSinkContext kafkaSinkContext, Long aLong) {
        long ts = System.currentTimeMillis();
        Set<TimestampedUIDTuple> lineage = genealogGraphTraverser.getProvenance(tuple);
        String lineageStr = FormatLineage.formattedLineage(lineage);
        tuple.setStimulusList(ts);
        // String latency = Long.toString(System.nanoTime() - tuple.getStimulus());
        return new ProducerRecord<>(topic, (FormatStimulusList.formatStimulusList(tuple.getStimulusList()) + "," + tuple.getStimulus() + ", Lineage(" + lineage.size() + ")" + lineageStr + ", OUT:" + tuple).getBytes(StandardCharsets.UTF_8));
    }
}
