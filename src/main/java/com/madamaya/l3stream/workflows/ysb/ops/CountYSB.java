package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple3<String, Long, Long>, YSBResultTuple> {
    @Override
    public Tuple3<String, Long, Long> createAccumulator() {
        return Tuple3.of("", 0L, 0L);
    }

    @Override
    public Tuple3<String, Long, Long> add(YSBInternalTuple tuple, Tuple3<String, Long, Long> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        acc.f2 = Math.max(acc.f2, tuple.getEventtime());
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple3<String, Long, Long> acc) {
        return new YSBResultTuple(acc.f0, acc.f1, acc.f2);
    }

    @Override
    public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> acc1, Tuple3<String, Long, Long> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
