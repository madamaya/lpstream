package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.l3stream.conf.L3conf;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ReplayMonitorTest {
    // public static boolean parseFlag = false;
    public static void main(String[] args) throws Exception {
        //String line = "{\"OUT\": [\"{\"DATA\": 1}\", \"{\"DATA\": 2}\"]}";
        String line = "\"hogehogehoge\"";
        System.out.println(line);
        System.out.println(line.substring(1, line.length() - 1));
    }
}
