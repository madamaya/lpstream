package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignNYC implements MapFunction<NYCInputTuple, NYCInputTuple> {
    @Override
    public NYCInputTuple map(NYCInputTuple in) throws Exception {
        NYCInputTuple out = new NYCInputTuple(in);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
