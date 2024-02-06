package com.madamaya.l3stream.workflows.unused.testold;

public class Test {
    public static void main(String[] args) throws Exception {
        /*
        String str = "2001-01-01 00:16:31";
        long ts = NYCInputTuple.convertDateFormat(str);
        System.out.println(ts);

        long ts2 = NexmarkInputTuple.convertDateStrToLong(str);
        System.out.println(ts2);
         */

        double d = 1.23456789;
        long st = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            String e = Double.toString(d);
            // String e = String.format("%.3f", d);
        }
        long ed = System.currentTimeMillis();

        System.out.println(ed - st);
    }
}
