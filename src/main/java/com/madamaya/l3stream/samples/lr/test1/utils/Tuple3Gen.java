package com.madamaya.l3stream.samples.lr.test1.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class Tuple3Gen implements MapFunction<String, Tuple3<Integer, Integer, Long>> {
    @Override
    public Tuple3<Integer, Integer, Long> map(String input) throws Exception {
        String[] elements = input.replace(" ", "").replace("[", "").replace("]", "").split(",");
        if (elements.length != 3) {
            return null;
        }
        int key = Integer.parseInt(elements[0]);
        int value = Integer.parseInt(elements[1]);
        long ts = Long.parseLong(elements[2]);

        return Tuple3.of(key, value, ts);
    }
}
