package com.madamaya.l3stream.getLineage;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FindJobName {
    /*
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("-1");
        }
        String jobid = args[0];
        System.out.println("jobid = " + jobid);

        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8081/jobs")).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println(response.statusCode());
        System.out.println(response.body());

        System.out.println(getJobName(jobid));
    }
     */

    public static String getJobName(String jobid) throws Exception {
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8081/jobs/" + jobid)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // System.out.println(response.statusCode());
        // System.out.println(response.body());

        JsonNode jsonNode = new ObjectMapper().readTree(response.body());
        return jsonNode.get("name").asText();
    }
}
