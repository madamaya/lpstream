package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple4<Integer, Long, Long, Double>, NYCResultTuple> {
    @Override
    public Tuple4<Integer, Long, Long, Double> createAccumulator() {
        return Tuple4.of(-1, -1L, 0L, 0.0);
    }

    @Override
    public Tuple4<Integer, Long, Long, Double> add(NYCInputTuple tuple, Tuple4<Integer, Long, Long, Double> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple4<Integer, Long, Long, Double> acc) {
        return new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2);
    }

    @Override
    public Tuple4<Integer, Long, Long, Double> merge(Tuple4<Integer, Long, Long, Double> acc1, Tuple4<Integer, Long, Long, Double> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
