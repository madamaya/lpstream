package com.madamaya.l3stream.utils.runnables;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.l3stream.conf.L3conf;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class Monitor implements Runnable {
    private long outputTs;
    private String lineageTopic;
    private String outputValue = "";
    private String query;
    private String size;
    private String experimentID;
    private int parallelism;
    private int partition;
    private long startTime;

    public Monitor(long outputTs, String lineageTopic, String outputValue, String query, String size, String experimentID, int parallelism, int partition, long startTime) {
        this.outputTs = outputTs;
        this.lineageTopic = lineageTopic;
        this.outputValue = outputValue;
        this.query = query;
        this.size = size;
        this.experimentID = experimentID;
        this.parallelism = parallelism;
        this.partition = partition;
        this.startTime = startTime;
    }

    @Override
    public void run() {
        System.out.println("Monitor (start): " + partition);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "monitor-" + System.currentTimeMillis());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> list = new ArrayList<>();
        list.add(new TopicPartition(lineageTopic, partition));
        consumer.assign(list);
        consumer.seekToBeginning(list);

        int count = 0;
        long endTime = -1;
        boolean active = true;
        boolean lineageDerivationFlag = false;
        boolean findFlag = false;
        try {
            System.out.println("Monitor.java: READY (" + partition + ")");
            while (!Thread.interrupted() && active) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    count++;
                    String currentRecord = (String) record.value();
                    if (lineageTopic.equals("Syn10-l")) {
                        findFlag = checkSame(currentRecord, outputValue, outputTs,
                                new Function<String, String>() {
                                    @Override
                                    public String apply(String s) {
                                        String[] elements = s.split(",");
                                        return elements[0] + "," + elements[1] + "," + elements[2] + "," + elements[4] + "," + elements[5];
                                    }
                                });
                    } else {
                        findFlag = checkSame(currentRecord, outputValue, outputTs);
                    }
                    if (findFlag) {
                        writeLineage(currentRecord, query, size, experimentID);
                        endTime = System.currentTimeMillis();
                        System.out.println("count = " + count + " [END] (" + partition + ")");
                        active = false;
                        lineageDerivationFlag = true;
                        break;
                    }
                }
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException();
        } catch (InterruptException e) {
            System.out.println("Monitor (interrupted): " + partition);
        }

        if (lineageDerivationFlag) {
            BufferedWriter bw;
            try {
                String dataPath = L3conf.L3_HOME + "/data/output/lineage/" + query;
                bw = new BufferedWriter(new FileWriter(dataPath + "/" + size + "-" + "monitor.log", true));
                bw.write(experimentID + "," + startTime + "," + endTime + "," + partition + "\n");
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Monitor (END): " + partition);
    }

    /* Sample data (recordValue) */
    // "Lineage([lineageSize]){[tuple1],...,[tupleN]},[outputTuple],[timestamp],[isReliable]"
    private static Tuple3<String, Long, Boolean> extractOutTsFlag(String recordValue) throws JsonProcessingException {
        // Make [ "Lineage([lineageSize]){[tuple1],...,[tupleN]", "[outputTuple],[timestamp],[isReliable]" ]
        String[] elements = recordValue.split("],", 2);

        // Extract the 2nd element and split it into values.
        int beforeFlagCommaIdx = elements[1].lastIndexOf(",");
        int beforeTsCommaIdx = elements[1].lastIndexOf(",", beforeFlagCommaIdx-1);
        String outputString = elements[1].substring(0, beforeTsCommaIdx);
        long ts = Long.parseLong(elements[1].substring(beforeTsCommaIdx+1, beforeFlagCommaIdx));
        boolean reliable = Boolean.parseBoolean(elements[1].substring(beforeFlagCommaIdx+1));

        return Tuple3.of(outputString, ts, reliable);
    }

    private static boolean checkSame(String recordValue, String outputValue, long outputTs) throws JsonProcessingException {
        return checkSame(recordValue, outputValue, outputTs, t -> t);
    }

    private static boolean checkSame(String recordValue, String outputValue, long outputTs, Function<String, String> func) throws JsonProcessingException {
        Tuple3<String, Long, Boolean> t3 = extractOutTsFlag(recordValue);
        return func.apply(t3.f0).equals(func.apply(outputValue)) && t3.f1 == outputTs && t3.f2;
    }

    private static void writeLineage(String recordValue, String query, String size, String experimentID) {
        String filePath = L3Config.L3_HOME + "/data/lineage/" + query + "/" + size + "_" + experimentID + ".txt";
        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(filePath));
            bf.write(recordValue);
            bf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
