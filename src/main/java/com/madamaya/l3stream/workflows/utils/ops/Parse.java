package com.madamaya.l3stream.workflows.utils.ops;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class Parse implements MapFunction<String, Tuple3<Integer, Long, Long>> {
    @Override
    public Tuple3<Integer, Long, Long> map(String s) throws Exception {
        String[] elements = s.split(",");
        if (elements.length < 4) {
            throw new IllegalStateException();
        }
        int partition = Integer.parseInt(elements[0]);
        long endTime = Long.parseLong(elements[1]);
        long startTime = Long.parseLong(elements[3]);

        return Tuple3.of(partition, startTime, endTime);
    }
}
