package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.*;

public class CountAndAvgDistance implements AggregateFunction<NYCInputTuple, Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency>, NYCResultTuple> {
    @Override
    public Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> createAccumulator() {
        return Tuple6.of(-1, -1L, 0L, 0.0, -1L, null);
    }

    @Override
    public Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> add(NYCInputTuple tuple, Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> acc) {
        acc.f0 = tuple.getVendorId();
        acc.f1 = tuple.getDropoffLocationId();
        acc.f2++;
        acc.f3 += tuple.getTripDistance();
        acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
        if (acc.f5 == null) {
            acc.f5 = tuple.getTfl();
        } else {
            if (acc.f5.ts2 >= tuple.getTfl().ts2) {
                acc.f5.setTs1(Math.max(acc.f5.ts1, tuple.getTfl().ts1));
            } else if (acc.f5.ts2 < tuple.getTfl().ts2) {
                long tmp = Math.max(acc.f5.ts1, tuple.getTfl().ts1);
                acc.f5 = tuple.getTfl();
                acc.f5.setTs1(tmp);
            } else {
                throw new IllegalStateException();
            }
        }
        return acc;
    }

    @Override
    public NYCResultTuple getResult(Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> acc) {
        NYCResultTuple out = new NYCResultTuple(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4);
        out.setTfl(acc.f5);
        return out;
    }

    @Override
    public Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> merge(Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> acc1, Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistance: merge()");
    }
}
