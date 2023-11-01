package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IdentifyEnd {
    public static void main(String[] args) throws Exception {
        assert args.length == 3;

        String topicName = args[0];
        int parallelism = Integer.parseInt(args[1]);
        int checkInterval = Integer.parseInt(args[2]);


        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toString(System.currentTimeMillis()));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<TopicPartition> list = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            list.add(new TopicPartition(topicName, i));
        }
        consumer.assign(list);
        consumer.seekToBeginning(list);
        Thread.sleep(30000);
        long count = 0;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(checkInterval));
            if (records.count() == 0) {
                endTime = System.currentTimeMillis();
                System.out.println("\r" + count + " tuple(s) have been read. (" + (endTime - startTime) + " [ms]) [end]");
                break;
            }
            for (ConsumerRecord record : records) {
                if (++count % 100 == 0) {
                    System.out.print("\r" + count + " tuple(s) have been read. (" + (System.currentTimeMillis() - startTime) + " [ms])");
                }
            }
        }
    }
}
