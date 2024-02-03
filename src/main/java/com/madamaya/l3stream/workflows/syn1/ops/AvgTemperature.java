package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class AvgTemperature implements AggregateFunction<SynTempTuple, Tuple6<Integer, Double, Long, Long, Long, Long>, SynResultTuple> {
    @Override
    public Tuple6<Integer, Double, Long, Long, Long, Long> createAccumulator() {
        return Tuple6.of(-1, 0.0, 0L, -1L, -1L, -1L);
    }

    @Override
    public Tuple6<Integer, Double, Long, Long, Long, Long> add(SynTempTuple tuple, Tuple6<Integer, Double, Long, Long, Long, Long> acc) {
        acc.f0 = tuple.getMachineId();
        acc.f1 = acc.f1 + tuple.getTemperature();
        acc.f2++;
        acc.f3 = Math.max(acc.f3, tuple.getTimestamp());
        acc.f4 = Math.max(acc.f4, tuple.getKafkaAppendTime());
        acc.f5 = Math.max(acc.f5, tuple.getStimulus());
        return acc;
    }

    @Override
    public SynResultTuple getResult(Tuple6<Integer, Double, Long, Long, Long, Long> acc) {
        return new SynResultTuple(acc.f0, acc.f1 / acc.f2, acc.f3, acc.f4, acc.f5);
    }

    @Override
    public Tuple6<Integer, Double, Long, Long, Long, Long> merge(Tuple6<Integer, Double, Long, Long, Long, Long> acc1, Tuple6<Integer, Double, Long, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("AvgTemperature: merge()");
    }
}
