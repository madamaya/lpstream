package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.l3stream.conf.L3conf;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ReplayMonitor {
    static ObjectMapper om = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        long outputTs;
        String lineageTopic;
        String outputValue = "";
        String query;
        String size;
        String experimentID;
        int parallelism;

        if (args.length == 7 && args[2].length() > 0) {
            outputTs = Long.parseLong(args[0]);
            lineageTopic = args[1];
            outputValue = args[2];
            query = args[3];
            size = args[4];
            experimentID = args[5];
            parallelism = Integer.parseInt(args[6]);
        } else {
            throw new IllegalArgumentException();
        }
        long startTime = System.currentTimeMillis();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "replaymonitor-" + System.currentTimeMillis());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<TopicPartition> list = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            list.add(new TopicPartition(lineageTopic, i));
        }
        consumer.assign(list);
        consumer.seekToBeginning(list);
        // consumer.subscribe(Arrays.asList(lineageTopic));

        int count = 0;
        boolean run = true;
        long endTime = -1;

        BufferedWriter bwDebug;
        try {
            bwDebug = new BufferedWriter(new FileWriter(L3conf.L3_HOME + "/data/output/lineage/" + query + "/" + System.currentTimeMillis() + ".log"));
            System.out.println("ReplayMonitor.java: READY");
            while (run) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    count++;
                    String currentRecord = (String) record.value();
                    bwDebug.write(currentRecord + "\n");
                    bwDebug.flush();
                    if (count % 1 == 0) {
                        System.out.print("\rcount = " + count);
                    }
                    if (checkSame(currentRecord, outputValue, outputTs)) {
                        System.out.println("count = " + count + " [END]");
                        writeLineage(currentRecord, query, size, experimentID);
                        endTime = System.currentTimeMillis();
                        run = false;
                        break;
                    }
                }

            }
            bwDebug.close();
        } catch (Exception e) {
            throw new RuntimeException();
        }

        BufferedWriter bw;
        try {
            String dataPath = L3conf.L3_HOME + "/data/output/lineage/" + query;
            bw = new BufferedWriter(new FileWriter(dataPath + "/" + size + "-" + "monitor.log", true));
            bw.write(experimentID + "," + startTime + "," + endTime + "\n");
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        Tuple3<String, Long, Boolean> t3 = extractOutTsFlag(recordValue);
        return t3.f0.equals(outputValue) && t3.f1 == outputTs && t3.f2;
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
        System.out.println("Lineage = " + recordValue);
    }
}
