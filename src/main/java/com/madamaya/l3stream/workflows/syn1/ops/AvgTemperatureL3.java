package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class AvgTemperatureL3 implements AggregateFunction<SynTempTuple, Tuple4<Integer, Double, Long, Long>, SynResultTuple> {
    @Override
    public Tuple4<Integer, Double, Long, Long> createAccumulator() {
        return Tuple4.of(-1, 0.0, 0L, -1L);
    }

    @Override
    public Tuple4<Integer, Double, Long, Long> add(SynTempTuple tuple, Tuple4<Integer, Double, Long, Long> acc) {
        acc.f0 = tuple.getMachineId();
        acc.f1 = acc.f1 + tuple.getTemperature();
        acc.f2++;
        acc.f3 = Math.max(acc.f3, tuple.getTimestamp());
        return acc;
    }

    @Override
    public SynResultTuple getResult(Tuple4<Integer, Double, Long, Long> acc) {
        return new SynResultTuple(acc.f0, acc.f1 / acc.f2, acc.f3);
    }

    @Override
    public Tuple4<Integer, Double, Long, Long> merge(Tuple4<Integer, Double, Long, Long> acc1, Tuple4<Integer, Double, Long, Long> acc2) {
        throw new UnsupportedOperationException("AvgTemperature: merge()");
    }
}
