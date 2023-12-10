package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTupleGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.function.Supplier;

public class CountYSBGL implements AggregateFunction<YSBInternalTupleGL, CountYSBGL.Accumulator, YSBResultTupleGL> {
    private Supplier<ProvenanceAggregateStrategy> strategySupplier;

    public CountYSBGL(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
        this.strategySupplier = strategySupplier;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(strategySupplier);
    }

    @Override
    public Accumulator add(YSBInternalTupleGL tuple, Accumulator acc) {
        acc.add(tuple);
        return acc;
    }

    @Override
    public YSBResultTupleGL getResult(Accumulator acc) {
        return acc.getAggregatedResult();
    }

    @Override
    public Accumulator merge(Accumulator acc1, Accumulator acc2) {
        throw new UnsupportedOperationException("CountYSBGL: merge()");
    }

    public static class Accumulator extends
            GenealogAccumulator<YSBInternalTupleGL, YSBResultTupleGL, Accumulator> {

        private Tuple4<String, Long, Long, TimestampsForLatency> acc = Tuple4.of("", 0L, 0L, null);

        public Accumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(YSBInternalTupleGL tuple) {
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
        }

        @Override
        protected YSBResultTupleGL doGetAggregatedResult() {
            YSBResultTupleGL out = new YSBResultTupleGL(acc.f0, acc.f1, acc.f2);
            out.setTfl(acc.f3);
            return out;
        }
    }
}
