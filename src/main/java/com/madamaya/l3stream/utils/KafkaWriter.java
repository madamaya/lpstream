package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaWriter {
    public static void main(String[] args) {

        String topic = "test";
        int partition = 0;

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<String, String>(topic, j, "", String.valueOf(10 * j + i)),
                        (recordMetadata, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        });
                System.out.println(i + "," + j);
            }
        }
        producer.close();
    }
}
