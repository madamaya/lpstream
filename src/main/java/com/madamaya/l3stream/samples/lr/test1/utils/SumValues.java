package com.madamaya.l3stream.samples.lr.test1.utils;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class SumValues implements AggregateFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, Long>> {
    @Override
    public Tuple3<Integer, Long, Long> createAccumulator() {
        return Tuple3.of(0, 0L, 0L);
    }

    @Override
    public Tuple3<Integer, Long, Long> add(Tuple3<Integer, Integer, Long> tuple, Tuple3<Integer, Long, Long> acc) {
        acc.f0 = tuple.f0;
        // acc.f1 += tuple.f1;
        acc.f1 += 1;
        acc.f2 = Math.max(acc.f2, tuple.f2);
        return acc;
    }

    @Override
    public Tuple3<Integer, Long, Long> getResult(Tuple3<Integer, Long, Long> acc) {
        return Tuple3.of(acc.f0, acc.f1, acc.f2);
    }

    @Override
    public Tuple3<Integer, Long, Long> merge(Tuple3<Integer, Long, Long> acc1, Tuple3<Integer, Long, Long> acc2) {
        throw new UnsupportedOperationException();
    }
}