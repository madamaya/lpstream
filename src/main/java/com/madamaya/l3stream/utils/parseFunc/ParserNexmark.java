package com.madamaya.l3stream.utils.parseFunc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ParserNexmark implements InputParser {
    ObjectMapper om = new ObjectMapper();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    @Override
    public String attachTimestamp(String line, long ts) throws Exception {
        Date date = new Date(ts);
        JsonNode jsonNode = om.readTree(line);
        if (jsonNode.get("event_type").asInt() == 0) {
            ((ObjectNode) jsonNode.get("person")).put("dateTime", sdf.format(date));
        } else if (jsonNode.get("event_type").asInt() == 1) {
            ((ObjectNode) jsonNode.get("auction")).put("dateTime", sdf.format(date));
        } else if (jsonNode.get("event_type").asInt() == 2) {
            ((ObjectNode) jsonNode.get("bid")).put("dateTime", sdf.format(date));
        } else {
            throw new IllegalArgumentException();
        }
        return jsonNode.toString();
    }
}
