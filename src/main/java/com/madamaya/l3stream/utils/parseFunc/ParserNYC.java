package com.madamaya.l3stream.utils.parseFunc;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ParserNYC implements InputParser {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    @Override
    public String attachTimestamp(String line, long ts) {
        String[] elements = line.split(",");
        elements[2] = sdf.format(new Date(ts));
        return String.join(",", elements);
    }
}
