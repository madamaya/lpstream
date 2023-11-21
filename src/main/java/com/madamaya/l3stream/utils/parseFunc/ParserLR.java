package com.madamaya.l3stream.utils.parseFunc;

public class ParserLR implements InputParser {

    @Override
    public String attachTimestamp(String line, long ts) {
        String[] elements = line.split(",");
        elements[1] = String.valueOf(ts);
        return String.join(",", elements);
    }
}
