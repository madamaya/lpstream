package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AvgTemperatureL3 implements AggregateFunction<SynTempTuple, Tuple5<Integer, Double, Long, Long, List<SynTempTuple>>, SynResultTuple> {
    @Override
    public Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> createAccumulator() {
        return Tuple5.of(-1, 0.0, 0L, -1L, new ArrayList<>());
    }

    @Override
    public Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> add(SynTempTuple tuple, Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> acc) {
        acc.f0 = tuple.getMachineId();
        acc.f1 = acc.f1 + tuple.getTemperature();
        acc.f2++;
        acc.f3 = Math.max(acc.f3, tuple.getTimestamp());
        acc.f4.add(tuple);
        return acc;
    }

    @Override
    public SynResultTuple getResult(Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> acc) {
        return new SynResultTuple(acc.f0, acc.f1 / acc.f2, acc.f2, acc.f4, acc.f3);
    }

    @Override
    public Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> merge(Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> acc1, Tuple5<Integer, Double, Long, Long, List<SynTempTuple>> acc2) {
        throw new UnsupportedOperationException("AvgTemperature: merge()");
    }
}
