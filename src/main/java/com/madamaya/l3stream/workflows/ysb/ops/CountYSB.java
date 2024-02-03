package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple5<String, Long, Long, Long, Long>, YSBResultTuple> {
    @Override
    public Tuple5<String, Long, Long, Long, Long> createAccumulator() {
        return Tuple5.of("", 0L, 0L, -1L, -1L);
    }

    @Override
    public Tuple5<String, Long, Long, Long, Long> add(YSBInternalTuple tuple, Tuple5<String, Long, Long, Long, Long> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        acc.f2 = Math.max(acc.f2, tuple.getEventtime());
        acc.f3 = Math.max(acc.f3, tuple.getKafkaAppendTime());
        acc.f4 = Math.max(acc.f4, tuple.getStimulus());
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple5<String, Long, Long, Long, Long> acc) {
        return new YSBResultTuple(acc.f0, acc.f1, acc.f2, acc.f3, acc.f4);
    }

    @Override
    public Tuple5<String, Long, Long, Long, Long> merge(Tuple5<String, Long, Long, Long, Long> acc1, Tuple5<String, Long, Long, Long, Long> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
