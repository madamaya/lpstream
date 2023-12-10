package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

public class CountYSB implements AggregateFunction<YSBInternalTuple, Tuple4<String, Long, Long, TimestampsForLatency>, YSBResultTuple> {
    @Override
    public Tuple4<String, Long, Long, TimestampsForLatency> createAccumulator() {
        return Tuple4.of("", 0L, 0L, null);
    }

    @Override
    public Tuple4<String, Long, Long, TimestampsForLatency> add(YSBInternalTuple tuple, Tuple4<String, Long, Long, TimestampsForLatency> acc) {
        acc.f0 = tuple.getCampaignId();
        acc.f1++;
        acc.f2 = Math.max(acc.f2, tuple.getEventtime());
        if (acc.f3 == null) {
            acc.f3 = tuple.getTfl();
        } else {
            if (acc.f3.ts2 >= tuple.getTfl().ts2) {
                acc.f3.setTs1(Math.max(acc.f3.ts1, tuple.getTfl().ts1));
            } else if (acc.f3.ts2 < tuple.getTfl().ts2) {
                long tmp = Math.max(acc.f3.ts1, tuple.getTfl().ts1);
                acc.f3 = tuple.getTfl();
                acc.f3.setTs1(tmp);
            } else {
                throw new IllegalStateException();
            }
        }
        return acc;
    }

    @Override
    public YSBResultTuple getResult(Tuple4<String, Long, Long, TimestampsForLatency> acc) {
        YSBResultTuple out = new YSBResultTuple(acc.f0, acc.f1, acc.f2);
        out.setTfl(acc.f3);
        return out;
    }

    @Override
    public Tuple4<String, Long, Long, TimestampsForLatency> merge(Tuple4<String, Long, Long, TimestampsForLatency> acc1, Tuple4<String, Long, Long, TimestampsForLatency> acc2) {
        throw new UnsupportedOperationException("CountYSB: merge()");
    }
}
