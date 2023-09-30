package com.madamaya.l3stream.workflows.nyc.tests;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;

public class InputParseTest {
    public static void main(String[] args) {
        String str = "1,2018-02-01 00:04:42,2018-02-01 00:19:32,1,5.8,1,N,236,119,2,18.5,0.5,0.5,0.0,0.0,0.3,19.8,,";

        NYCInputTuple tuple = new NYCInputTuple(str);

        System.out.println(tuple);
    }
}
