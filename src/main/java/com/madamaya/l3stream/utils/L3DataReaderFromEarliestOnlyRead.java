package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

public class L3DataReaderFromEarliestOnlyRead {
    public static void main(String[] args) throws Exception {
        assert args.length == 2;

        for (int i = 0; i < args.length; i++)
            System.out.println(args[i]);
        String topicName = args[0];
        int parallelism = Integer.parseInt(args[1]);

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

        long count = 0;
        long prevCount = 0;
        long startTime = System.currentTimeMillis();
        long prevTime = startTime;
        final long checkInterval = 30000;

        Map<Integer, Long> map = new HashMap<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(checkInterval));
            if (System.currentTimeMillis() - prevTime > checkInterval) {
                if (prevCount == count) {
                    // System.out.println(" [end (All outputs have been read.)]");
                    break;
                } else if (prevCount < count) {
                    prevTime = System.currentTimeMillis();
                    prevCount = count;
                    System.out.print("\r" + count + " tuple(s) have been read. (" + (System.currentTimeMillis() - startTime) + " [ms])");
                }
            }

            for (ConsumerRecord record : records) {
                String recordValue = (String) record.value();
                long ts = record.timestamp();
                int partition = record.partition();
                if (++count % 100 == 0) {
                    System.out.print("\r" + count + " tuple(s) have been read.");
                }
                map.put(partition, map.getOrDefault(partition, 0L) + 1);
            }
        }
        System.out.println("\r" + count + " tuple(s) have been read. [end]");
        System.out.println("map = " + map);
    }
}
