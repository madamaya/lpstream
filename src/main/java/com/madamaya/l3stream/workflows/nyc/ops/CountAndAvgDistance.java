package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long>, NYCResultTuple> {
    @Override
    public Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> createAccumulator() {
        return Tuple8.of(-1, -1L, 0L, 0.0, -1L, -1L, -1L, -1L);
    }

    @Override
    public Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> add(NYCInputTuple tuple, Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
        acc.f5 = Math.max(acc.f5, tuple.getDominantOpTime());
        acc.f6 = Math.max(acc.f6, tuple.getKafkaAppendTime());
        acc.f7 = Math.max(acc.f7, tuple.getStimulus());
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> acc) {
        return new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4, System.nanoTime() - acc.f5, acc.f6, acc.f7);
    }

    @Override
    public Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> merge(Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> acc1, Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
