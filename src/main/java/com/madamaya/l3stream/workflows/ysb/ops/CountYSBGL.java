package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTupleGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBResultTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.List;
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

        private Tuple5<String, Long, Long, Long, List<Long>> acc = Tuple5.of("", 0L, 0L, -1L, null);

        public Accumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(YSBInternalTupleGL tuple) {
            acc.f0 = tuple.getCampaignId();
            acc.f1++;
            acc.f2 = Math.max(acc.f2, tuple.getEventtime());
            // acc.f3 = Math.max(acc.f3, tuple.getStimulus());
            if (acc.f4 == null || acc.f4.get(0) < tuple.getStimulusList().get(0)) {
                acc.f3 = System.currentTimeMillis();
                acc.f4 = tuple.getStimulusList();
            }
        }

        @Override
        protected YSBResultTupleGL doGetAggregatedResult() {
            YSBResultTupleGL tuple = new YSBResultTupleGL(acc.f0, acc.f1, acc.f2);
            tuple.setStimulusList(acc.f4);
            tuple.setStimulusList(acc.f3);
            tuple.setStimulusList(System.currentTimeMillis());
            return tuple;
        }
    }
}
