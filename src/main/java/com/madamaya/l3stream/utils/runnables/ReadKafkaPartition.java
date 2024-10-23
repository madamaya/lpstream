package com.madamaya.l3stream.utils.runnables;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ReadKafkaPartition implements Runnable {
    private String topicName;
    private int readPartition;
    private String outputFileDir;
    private String size;
    private boolean withLineage;
    private boolean isLatencyExperiment;
    private boolean isRawMode;

    public ReadKafkaPartition(String topicName, int readPartition, String outputFileDir, String size, boolean withLineage, boolean isLatencyExperiment, boolean isRawMode) {
        this.topicName = topicName;
        this.readPartition = readPartition;
        this.outputFileDir = outputFileDir;
        this.size = size;
        this.withLineage = withLineage;
        this.isLatencyExperiment = isLatencyExperiment;
        this.isRawMode = isRawMode;
    }

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Long.toString(System.currentTimeMillis()));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> list = new ArrayList<>();
        list.add(new TopicPartition(topicName, readPartition));
        consumer.assign(list);
        consumer.seekToBeginning(list);

        long count = 0;
        long prevCount = 0;
        long startTime = System.currentTimeMillis();
        long prevTime = startTime;
        final long checkInterval = 30000;
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileDir + "/" + size + "_" + readPartition + ".csv"));
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
                    if (readPartition != partition) {
                        throw new IllegalStateException();
                    }
                    if (!isRawMode) {
                        String[] elements = recordValue.split(",");
                        if (withLineage) {
                            long s2sLatency = Long.parseLong(elements[0]);
                            long k2kLatency = ts - Long.parseLong(elements[1]);
                            long domLatency = Long.parseLong(elements[2]);
                            long traverseTime = Long.parseLong(elements[3]);
                            String line = partition + "," + ts + "," + s2sLatency + "," + k2kLatency + "," + domLatency + "," + traverseTime;
                            if (isLatencyExperiment) {
                                // 4 is the number of ','.
                                int outputSize = recordValue.length() - elements[0].length() - elements[1].length() - elements[2].length() - elements[3].length() - 4;
                                int lineageSize = Integer.parseInt(recordValue.split(",Lineage\\(")[1].split("\\)\\[")[0]);
                                line += ("," + outputSize + "," + lineageSize);
                            }
                            bw.write(line + "\n");
                        } else {
                            long s2sLatency = Long.parseLong(elements[0]);
                            long k2kLatency = ts - Long.parseLong(elements[1]);
                            long domLatency = Long.parseLong(elements[2]);
                            // 3 is the number of ','.
                            int outputSize = recordValue.length() - elements[0].length() - elements[1].length() - elements[2].length() - 3;
                            bw.write(partition + "," + ts + "," + s2sLatency + "," + k2kLatency + "," + domLatency + "," + outputSize + "\n");
                        }
                    } else {
                        bw.write(recordValue + "\n");
                    }

                    if (++count % 100000 == 0) {
                        System.out.print("\r" + count + " tuple(s) have been read.");
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            bw.close();

            System.out.println("\r" + count + " tuple(s) have been read. [end]");
            System.out.println("Time duration: " + (endTime-startTime) + "[ms]");

            BufferedWriter bw2 = new BufferedWriter(new FileWriter(outputFileDir + "/" + size + "_" + readPartition + "_log.log"));
            bw2.write("datanum at partition(" + readPartition + "): " + count + "\n");
            bw2.write("Time duration: " + (endTime-startTime) + "[ms]\n");
            bw2.close();
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
