package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.List;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple5<String, Long, Long, Long, List<Long>>, YSBResultTuple> {
    @Override
    public Tuple5<String, Long, Long, Long, List<Long>> createAccumulator() {
        return Tuple5.of("", 0L, 0L, -1L, null);
    }

    @Override
    public Tuple5<String, Long, Long, Long, List<Long>> add(YSBInternalTuple tuple, Tuple5<String, Long, Long, Long, List<Long>> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        acc.f2 = Math.max(acc.f2, tuple.getEventtime());
        // acc.f3 = Math.max(acc.f3, tuple.getStimulus());
        if (acc.f4 == null || acc.f4.get(0) < tuple.getStimulusList().get(0)) {
            acc.f3 = System.currentTimeMillis();
            acc.f4 = tuple.getStimulusList();
        }
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple5<String, Long, Long, Long, List<Long>> acc) {
        YSBResultTuple tuple = new YSBResultTuple(acc.f0, acc.f1, acc.f2);
        tuple.setStimulusList(acc.f4);
        tuple.setStimulusList(acc.f3);
        tuple.setStimulusList(System.currentTimeMillis());
        return tuple;
    }

    @Override
    public Tuple5<String, Long, Long, Long, List<Long>> merge(Tuple5<String, Long, Long, Long, List<Long>> acc1, Tuple5<String, Long, Long, Long, List<Long>> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
