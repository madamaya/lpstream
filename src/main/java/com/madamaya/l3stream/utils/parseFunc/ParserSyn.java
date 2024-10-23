package com.madamaya.l3stream.utils.parseFunc;

public class ParserSyn implements InputParser {

    @Override
    public String attachTimestamp(String line, long ts) {
        return line + "," + ts;
    }
}
