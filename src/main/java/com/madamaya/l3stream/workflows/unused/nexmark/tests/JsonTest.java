package com.madamaya.l3stream.workflows.unused.nexmark.tests;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Paths;

public class JsonTest {
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(Paths.get("/Users/yamada-aist/workspace/l3stream/src/main/java/com/madamaya/l3stream/workflows/nexmark/tests/test.json").toFile());
        System.out.println(json);
        System.out.println(json.get("auction"));
        System.out.println(json.get("auction").asText().equals("null"));
    }
}
