package com.madamaya.l3stream.utils.parseFunc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class ParserYSB implements InputParser {
    ObjectMapper om = new ObjectMapper();
    @Override
    public String attachTimestamp(String line, long ts) throws Exception {
        om.readTree(line);
        String[] head = line.split("\"event_time\"");
        String[] tail = line.split("\"ip_address\"");
        String sentLine = head[0] + "\"event_time\": \"" + ts + "\", \"ip_address\"" + tail[1];
        return sentLine;
    }
}
