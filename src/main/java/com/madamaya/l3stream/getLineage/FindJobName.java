package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FindJobName {
    public static String getJobName(String jobid) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://" + L3Config.FLINK_IP + ":" + L3Config.FLINK_PORT + "/jobs/" + jobid)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode jsonNode = new ObjectMapper().readTree(response.body());
        return jsonNode.get("name").asText();
    }
}
