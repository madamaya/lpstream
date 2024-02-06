package com.madamaya.l3stream.workflows.unused.testold;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonNodeTest {
    public static void main(String[] args) throws Exception {
        ObjectMapper om = new ObjectMapper();
        om.readTree("{\"OUT\": 1}");
        System.out.println(om);
        om.readTree("{\"OUT\": 2}");
        System.out.println(om);
    }
}