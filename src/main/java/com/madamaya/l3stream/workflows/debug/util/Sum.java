package com.madamaya.l3stream.workflows.debug.util;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Sum implements AggregateFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {
    @Override
    public Tuple3<Integer, Integer, String> createAccumulator() {
        return Tuple3.of(0, 0, "[");
    }

    @Override
    public Tuple3<Integer, Integer, String> add(Tuple3<Integer, Integer, Integer> t, Tuple3<Integer, Integer, String> acc) {
        acc.f0 = t.f0;
        acc.f1 += t.f1;
        acc.f2 += t.toString() + ",";
        return acc;
    }

    @Override
    public Tuple3<Integer, Integer, String> getResult(Tuple3<Integer, Integer, String> acc) {
        return Tuple3.of(acc.f0, acc.f1, acc.f2 + "]");
    }

    @Override
    public Tuple3<Integer, Integer, String> merge(Tuple3<Integer, Integer, String> acc1, Tuple3<Integer, Integer, String> acc2) {
        throw new UnsupportedOperationException();
    }
}
