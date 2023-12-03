package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

import java.util.List;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>>, NYCResultTuple> {
    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> createAccumulator() {
        // return Tuple6.of(-1, -1L, 0L, 0.0, -1L, -1L);
        return Tuple7.of(-1, -1L, 0L, 0.0, -1L, -1L, null);
    }

    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> add(NYCInputTuple tuple, Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
        if (acc.f6 == null || acc.f6.get(0) < tuple.getStimulusList().get(0)) {
            acc.f5 = System.currentTimeMillis();
            acc.f6 = tuple.getStimulusList();
        }
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> acc) {
        long ts = System.currentTimeMillis();
        NYCResultTuple tuple = new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4);
        tuple.setStimulusList(acc.f6);
        tuple.setStimulusList(acc.f5);
        tuple.setStimulusList(ts);
        return tuple;
    }

    @Override
    public Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> merge(Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> acc1, Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
