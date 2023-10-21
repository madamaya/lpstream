package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ReplayMonitor {
    public static boolean parseFlag = false;

    public static void main(String[] args) throws Exception {
        long outputTs;
        String lineageTopic;
        String outputValue = "";

        if (args.length == 2) {
            outputTs = Long.parseLong(args[0]);
            lineageTopic = args[1];
        } else if (args.length == 3) {
            outputTs = Long.parseLong(args[0]);
            lineageTopic = args[1];
            // remove "\"" from beginnig and end of the input
            outputValue = args[2].substring(0, args[2].length()-1).substring(1);
        } else {
            throw new IllegalArgumentException();
        }
        parseFlag = lineageTopic.contains("Nexmark");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, L3Config.BOOTSTRAP_IP_PORT);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "replaymonitor-" + System.currentTimeMillis());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(lineageTopic));

        int count = 0;
        boolean run = true;
        while (run) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                count++;
                String recordValue = (String) record.value();
                if (count % 10 == 0) {
                    System.out.print("\rcount = " + count);
                }
                if (checkSame(recordValue, outputValue, outputTs)) {
                    writeLineage((String) record.value());
                    cancelJob();
                    run = false;
                    break;
                }
            }
        }
    }

    /* Sample data (recordValue) */
    // {
    //  "OUT":"NYCResultTuple{vendorId=1, dropoffLocationId=164, count=3, avgDistance=13.666666666666666, ts=1642839653000}",
    //  "TS":"1642839653000",
    //  "CPID":"-1",
    //  "LINEAGE":[
    //        {"value":"1,2022-01-22 16:34:03,2022-01-22 17:14:32,1.0,17.5,2.0,N,132,164,1,52.0,2.5,0.5,15.45,6.55,0.3,77.3,2.5,0.0","metadata":{"offset":429563,"topic":"NYC-i","partition":3}},
    //        {"value":"1,2022-01-22 16:42:16,2022-01-22 17:20:53,1.0,18.0,2.0,N,132,164,1,52.0,3.75,0.5,10.0,6.55,0.3,73.1,2.5,1.25","metadata":{"offset":429777,"topic":"NYC-i","partition":3}},
    //        {"value":"1,2022-01-22 16:42:11,2022-01-22 17:09:25,2.0,5.5,1.0,Y,151,164,1,23.0,2.5,0.5,5.25,0.0,0.3,31.55,2.5,0.0","metadata":{"offset":412428,"topic":"NYC-i","partition":2}}
    //  ],
    //  "FLAG":"true"
    // }
    public static Tuple3<String, Long, Boolean> extractOutTsFlag(String recordValue) throws JsonProcessingException {
        // CNFM
        JsonNode jsonNode;
        if (parseFlag) {
            jsonNode = new ObjectMapper().readTree(recordValue.replace("\\", ""));
        } else {
            jsonNode = new ObjectMapper().readTree(recordValue);
        }
        return Tuple3.of(jsonNode.get("OUT").asText(), jsonNode.get("TS").asLong(), jsonNode.get("FLAG").asBoolean());
    }

    public static boolean checkSame(String recordValue, String outputValue, long outputTs) throws JsonProcessingException {
        Tuple3<String, Long, Boolean> t3 = extractOutTsFlag(recordValue);
        if (outputValue.length() < 1) {
            // check timestamp and flag
            return t3.f1 == outputTs && t3.f2;
        } else {
            // check timestamp, value, and flag
            // CNFM
            if (parseFlag) {
                return t3.f0.equals(outputValue.replace("\\", "")) && t3.f1 == outputTs && t3.f2;
            } else {
                return t3.f0.equals(outputValue) && t3.f1 == outputTs && t3.f2;
            }
        }
    }

    public static void writeLineage(String recordValue) {
        // CNFM
        System.out.println("!!! This is a tentative implementation of writeLineage (ReplayMonitor.java). !!!");
        String filePath = L3Config.L3_HOME + "/data/lineage/" + System.currentTimeMillis() + ".txt";
        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(filePath));
            bf.write(recordValue);
            bf.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Lineage = " + recordValue);
    }

    public static void cancelJob() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().build();

        // Create running jobid list, which should be canceled
        HttpRequest request = HttpRequest
                .newBuilder()
                .uri(URI.create("http://" + L3Config.FLINK_IP + ":" + L3Config.FLINK_PORT + "/jobs/overview"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode jsonNode = new ObjectMapper().readTree(response.body());
        List<String> endIDs = new ArrayList<>();
        for (int idx = 0; idx < jsonNode.get("jobs").size(); idx++) {
            JsonNode current = jsonNode.get("jobs").get(idx);
            String currentState = current.get("state").asText();
            String currentName = current.get("name").asText().split(",")[0];
            if (currentState.equals("RUNNING") && currentName.equals("LineageMode")) {
                endIDs.add(current.get("jid").asText());
            }
        }

        // Cancel flink job via REST API
        for (int idx = 0; idx < endIDs.size(); idx++) {
            request = HttpRequest
                    .newBuilder()
                    .uri(URI.create("http://" + L3Config.FLINK_IP + ":" + L3Config.FLINK_PORT + "/jobs/" + endIDs.get(idx)))
                    .method("PATCH", HttpRequest.BodyPublishers.noBody())
                    .build();
            client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }
}
