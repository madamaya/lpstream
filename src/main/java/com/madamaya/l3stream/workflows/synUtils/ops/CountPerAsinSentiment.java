package com.madamaya.l3stream.workflows.synUtils.ops;

import com.madamaya.l3stream.workflows.synUtils.objects.SynInternalTuple;
import com.madamaya.l3stream.workflows.synUtils.objects.SynResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountPerAsinSentiment implements AggregateFunction<SynInternalTuple, Tuple3<String, Integer, Long>, SynResultTuple> {

    @Override
    public Tuple3<String, Integer, Long> createAccumulator() {
        return Tuple3.of("", 0, 0L);
    }

    @Override
    public Tuple3<String, Integer, Long> add(SynInternalTuple tuple, Tuple3<String, Integer, Long> acc) {
        acc.f0 = tuple.getAsin();
        acc.f1 = tuple.getSentiment();
        acc.f2++;
        return acc;
    }

    @Override
    public SynResultTuple getResult(Tuple3<String, Integer, Long> acc) {
        return new SynResultTuple(acc.f0, acc.f1, acc.f2);
    }

    @Override
    public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> acc1, Tuple3<String, Integer, Long> acc2) {
        throw new UnsupportedOperationException("CountPerAsinSentiment: merge()");
    }
}
