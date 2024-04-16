package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class LatencyCalcFromKafka {
    public static void main(String[] args) throws Exception {
        assert args.length == 3;

        String topicName = args[0];
        int parallelism = Integer.parseInt(args[1]);
        String outputFilePath = args[2];

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toString(System.currentTimeMillis()));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // Case1: Use subscribe() function
        // Read data from latest offset not earliest offset
        // consumer.subscribe(Arrays.asList(topicName));

        // Case2: Use seekToBeginning() function
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
        Map<Integer, List<Tuple2<Long, Long>>> latencies = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(checkInterval));
            if (System.currentTimeMillis() - prevTime > checkInterval && prevCount == count) {
                break;
            }

            prevTime = System.currentTimeMillis();
            prevCount = count;

            for (ConsumerRecord record : records) {
                String recordValue = (String) record.value();
                long ts = record.timestamp();
                int partition = record.partition();
                long latency = Long.parseLong(recordValue.split(",")[0]);

                map.put(partition, map.getOrDefault(partition, 0L) + 1);
                if (!latencies.containsKey(partition)) {
                    latencies.put(partition, new ArrayList<>());
                }
                latencies.get(partition).add(Tuple2.of(ts, latency));

                if (++count % 100000 == 0) {
                    System.out.print("\r" + count + " tuple(s) have been read.");
                }
            }
        }
        long endTime = System.currentTimeMillis();

        System.out.println("\r" + count + " tuple(s) have been read. [end]");
        System.out.println("datanum per partition: " + map);
        System.out.println("Time duration: " + (endTime-startTime) + "[ms]");
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath));
            bw.write("datanum per partition: " + map + "\n");
            bw.write("Time duration: " + (endTime-startTime) + "[ms]\n");
            bw.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
