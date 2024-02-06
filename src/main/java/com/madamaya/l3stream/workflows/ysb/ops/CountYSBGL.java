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
import org.apache.flink.api.java.tuple.Tuple6;

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

        private Tuple6<String, Long, Long, Long, Long, Long> acc = Tuple6.of("", 0L, 0L, -1L, -1L, -1L);

        public Accumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(YSBInternalTupleGL tuple) {
            acc.f0 = tuple.getCampaignId();
            acc.f1++;
            acc.f2 = Math.max(acc.f2, tuple.getEventtime());
            acc.f3 = Math.max(acc.f3, tuple.getDominantOpTime());
            acc.f4 = Math.max(acc.f4, tuple.getKafkaAppendTime());
            acc.f5 = Math.max(acc.f5, tuple.getStimulus());
        }

        @Override
        protected YSBResultTupleGL doGetAggregatedResult() {
            return new YSBResultTupleGL(acc.f0, acc.f1, acc.f2, System.nanoTime() - acc.f3, acc.f4, acc.f5);
        }
    }
}
