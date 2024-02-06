package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.*;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple6<String, Long, Long, Long, Long, Long>, YSBResultTuple> {
    @Override
    public Tuple6<String, Long, Long, Long, Long, Long> createAccumulator() {
        return Tuple6.of("", 0L, 0L, -1L, -1L, -1L);
    }

    @Override
    public Tuple6<String, Long, Long, Long, Long, Long> add(YSBInternalTuple tuple, Tuple6<String, Long, Long, Long, Long, Long> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        acc.f2 = Math.max(acc.f2, tuple.getEventtime());
        acc.f3 = Math.max(acc.f3, tuple.getDominantOpTime());
        acc.f4 = Math.max(acc.f4, tuple.getKafkaAppendTime());
        acc.f5 = Math.max(acc.f5, tuple.getStimulus());
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple6<String, Long, Long, Long, Long, Long> acc) {
        return new YSBResultTuple(acc.f0, acc.f1, acc.f2, System.nanoTime() - acc.f3, acc.f4, acc.f5);
    }

    @Override
    public Tuple6<String, Long, Long, Long, Long, Long> merge(Tuple6<String, Long, Long, Long, Long, Long> acc1, Tuple6<String, Long, Long, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
