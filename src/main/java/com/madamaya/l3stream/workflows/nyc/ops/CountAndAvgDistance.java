package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple7<Integer, Long, Long, Double, Long, Long, Long>, NYCResultTuple> {
    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, Long> createAccumulator() {
        return Tuple7.of(-1, -1L, 0L, 0.0, -1L, -1L, -1L);
    }

    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, Long> add(NYCInputTuple tuple, Tuple7<Integer, Long, Long, Double, Long, Long, Long> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
        acc.f5 = Math.max(acc.f5, tuple.getKafkaAppendTime());
        acc.f6 = Math.max(acc.f6, tuple.getStimulus());
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple7<Integer, Long, Long, Double, Long, Long, Long> acc) {
        return new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4, acc.f5, acc.f6);
    }

    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, Long> merge(Tuple7<Integer, Long, Long, Double, Long, Long, Long> acc1, Tuple7<Integer, Long, Long, Double, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
