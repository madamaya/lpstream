package com.madamaya.l3stream.workflows.utils.ops;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class Deserializer implements KafkaRecordDeserializationSchema<String> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<String> collector) throws IOException {
        collector.collect(consumerRecord.partition() + "," + consumerRecord.timestamp() + "," + new String(consumerRecord.value()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
