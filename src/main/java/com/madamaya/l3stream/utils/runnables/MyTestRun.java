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

public class MyTestRun implements Runnable {
    String filePath;
    String qName;
    String topic;
    int partition;
    Map<Integer, Long> map;

    public MyTestRun(String filePath, String qName, String topic, int partition, Map<Integer, Long> map) {
        this.filePath = filePath;
        this.qName = qName;
        this.topic = topic;
        this.partition = partition;
        this.map = map;
    }

    @Override
    public void run() {
        InputParser ip;
        System.out.println("qName = " + qName);
        if (qName.equals("LR")) {
            ip = new ParserLR();
        } else if (qName.equals("Nexmark")) {
            ip = new ParserNexmark();
        } else if (qName.equals("NYC")) {
            ip = new ParserNYC();
        } else if (qName.equals("YSB")) {
            ip = new ParserYSB();
        } else {
            throw new IllegalArgumentException();
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        sendFromFile(filePath, ip, producer);
        // sendFromCache(filePath, ip, producer);
    }

    public void sendFromFile(String filePath, InputParser ip, KafkaProducer<String, String> producer) {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(filePath));
            String line;
            long dataNum = 0;
            long stime = System.nanoTime();
            while ((line = br.readLine()) != null) {
                String sendLine = ip.attachTimestamp(line, System.currentTimeMillis());
                producer.send(new ProducerRecord<String, String>(topic, partition, null, null, sendLine),
                        (recordMetadata, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        });
                dataNum++;
            }
            long etime = System.nanoTime();

            map.put(partition, dataNum);

            System.out.println("Duration: " + (etime - stime) / 1e9 + " [s]");
            System.out.println("Throughput: " + dataNum / ((etime - stime) / 1e9) + " [tuples/s]");
            System.out.println("DataNum: " + dataNum + " [tuples]");
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}