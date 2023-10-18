package com.madamaya.l3stream.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class L3DataLoader {
    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Arg error");
            System.exit(1);
        }

        String dataPath = args[0];
        String topic = args[1];

        System.out.println("dataPath=" + dataPath + ", topic=" + topic);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

 //       KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        File f = new File(dataPath);
        long count = 0;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<String, String>(topic, line),
                        (recordMetadata, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        });
                count++;
                if (count % 100000 == 0) {
                    System.out.print("\r" + count);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();

        System.out.println(" [END WRITE: " + count + " data were written.]");
    }
}
