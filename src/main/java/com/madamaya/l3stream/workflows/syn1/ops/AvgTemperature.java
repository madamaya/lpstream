package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class AvgTemperature implements AggregateFunction<SynTempTuple, Tuple5<Integer, Double, Long, Long, Long>, SynResultTuple> {
    @Override
    public Tuple5<Integer, Double, Long, Long, Long> createAccumulator() {
        return Tuple5.of(-1, 0.0, 0L, -1L, -1L);
    }

    @Override
    public Tuple5<Integer, Double, Long, Long, Long> add(SynTempTuple tuple, Tuple5<Integer, Double, Long, Long, Long> acc) {
        acc.f0 = tuple.getMachineId();
        acc.f1 = acc.f1 + tuple.getTemperature();
        acc.f2++;
        acc.f3 = Math.max(acc.f3, tuple.getTimestamp());
        acc.f4 = Math.max(acc.f4, tuple.getStimulus());
        return acc;
    }

    @Override
    public SynResultTuple getResult(Tuple5<Integer, Double, Long, Long, Long> acc) {
        return new SynResultTuple(acc.f0, acc.f1 / acc.f2, acc.f3, acc.f4);
    }

    @Override
    public Tuple5<Integer, Double, Long, Long, Long> merge(Tuple5<Integer, Double, Long, Long, Long> acc1, Tuple5<Integer, Double, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("AvgTemperature: merge()");
    }
}
