package com.madamaya.l3stream.workflows.unused.lr.test;

import org.apache.flink.api.common.time.Time;

public class TimestampTest {
    public static void main(String[] args) throws Exception {
        long ts = System.currentTimeMillis();
        System.out.println("ts = " + ts);
        System.out.println("Time.seconds(ts).toMilliseconds = " + Time.seconds(ts).toMilliseconds());
        System.out.println("Time.seconds(ts/1000).toMilliseconds = " + Time.seconds(ts/1000).toMilliseconds());
    }
}
