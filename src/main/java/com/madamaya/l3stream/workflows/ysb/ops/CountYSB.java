package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple2<String, Long>, YSBResultTuple> {
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return Tuple2.of("", 0L);
    }

    @Override
    public Tuple2<String, Long> add(YSBInternalTuple tuple, Tuple2<String, Long> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple2<String, Long> acc) {
        return new YSBResultTuple(acc.f0, acc.f1);
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
