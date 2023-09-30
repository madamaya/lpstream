package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple5<Integer, Long, Long, Double, Long>, NYCResultTuple> {
    @Override
    public Tuple5<Integer, Long, Long, Double, Long> createAccumulator() {
        return Tuple5.of(-1, -1L, 0L, 0.0, -1L);
    }

    @Override
    public Tuple5<Integer, Long, Long, Double, Long> add(NYCInputTuple tuple, Tuple5<Integer, Long, Long, Double, Long> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple5<Integer, Long, Long, Double, Long> acc) {
        return new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4);
    }

    @Override
    public Tuple5<Integer, Long, Long, Double, Long> merge(Tuple5<Integer, Long, Long, Double, Long> acc1, Tuple5<Integer, Long, Long, Double, Long> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
