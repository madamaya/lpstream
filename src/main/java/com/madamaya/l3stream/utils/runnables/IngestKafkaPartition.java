package com.madamaya.l3stream.utils.runnables;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.utils.parseFunc.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IngestKafkaPartition implements Runnable {
    String filePath;
    String qName;
    String topic;
    int partition;
    int throughput;
    Map<Integer, Double> map;
    final InputParser ip;
    final KafkaProducer<String, String> producer;
    int granularity = 1; // 1 -> 1000ms, 10 -> 100ms, 100 -> 10ms, etc.

    public IngestKafkaPartition(String filePath, String qName, String topic, int partition, int throughput, Map<Integer, Double> map) {
        this.filePath = filePath + ".ingest." + partition;
        // this.filePath = filePath;
        this.qName = qName;
        this.topic = topic;
        this.partition = partition;
        this.throughput = throughput;
        this.map = map;

        // initialize data parser
        if (qName.contains("LR")) {
            this.ip = new ParserLR();
        } else if (qName.contains("Nexmark")) {
            this.ip = new ParserNexmark();
        } else if (qName.equals("NYC")) {
            this.ip = new ParserNYC();
        } else if (qName.equals("YSB")) {
            this.ip = new ParserYSB();
        } else {
            throw new IllegalArgumentException();
        }

        // initialize kafkaProducer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(properties);
    }

    public IngestKafkaPartition(String filePath, String qName, String topic, int partition, int throughput, Map<Integer, Double> map, int granularity) {
        this(filePath, qName, topic, partition, throughput, map);
        this.granularity = granularity;
    }

    @Override
    public void run() {
        // sendFromFile();
        sendFromFileLoop();
    }

    public void sendFromFileLoop() {
        BufferedReader br;
        try {
            String line;
            long count = 0;
            long dataNum = 0;

            long stime = System.nanoTime();
            long prevTime = System.nanoTime();
            while (true) {
                br = new BufferedReader(new FileReader(filePath));
                while ((line = br.readLine()) != null) {
                    // Send data
                    String sendLine = ip.attachTimestamp(line, System.currentTimeMillis());
                    // System.out.println(sendLine);
                    producer.send(new ProducerRecord<String, String>(topic, partition, null, null, sendLine),
                            (recordMetadata, e) -> {
                                if (e != null) {
                                    e.printStackTrace();
                                }
                            });

                    // Check tupleNum sent for latest interval
                    if (++count == (throughput / granularity)) {
                        dataNum += count;
                        count = 0;

                        long sleepTime = 1000 / granularity - ((System.nanoTime() - prevTime) / 1000000);
                        if (sleepTime > 0) {
                            // System.out.println(sleepTime);
                            Thread.sleep(sleepTime);
                        } else {
                            // System.out.println("delay: " + sleepTime + " [ms]");
                            // prevTime = System.nanoTime();
                        }
                        prevTime += (1000000000 / granularity);
                        map.put(partition, dataNum / ((System.nanoTime() - stime) / 1e9));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}