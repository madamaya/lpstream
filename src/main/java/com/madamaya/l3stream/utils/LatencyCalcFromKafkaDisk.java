package com.madamaya.l3stream.utils;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.utils.runnables.ReadKafkaPartition;
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

public class LatencyCalcFromKafkaDisk {
    public static void main(String[] args) throws Exception {
        assert args.length == 3 || args.length == 4;
        String topicName = args[0];
        int parallelism = Integer.parseInt(args[1]);
        String outputFileDir = args[2];
        String key = (args.length == 4) ? args[3] : "";

        System.out.println("==== ARGS ====");
        System.out.println("\ttopicName = " + topicName);
        System.out.println("\tparallelism = " + parallelism);
        System.out.println("\toutputFileDir = " + outputFileDir);
        System.out.println("\tkey = " + key);
        System.out.println("==============");

        List<Thread> threadList = new ArrayList<>();
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("ADD: " + idx);
            threadList.add(new Thread(new ReadKafkaPartition(topicName, idx, outputFileDir, key)));
        }
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("START: " + idx);
            threadList.get(idx).start();
        }
        for (int idx = 0; idx < parallelism; idx++) {
            System.out.println("JOIN: " + idx);
            threadList.get(idx).join();
        }

        System.out.println("END");
    }
}
