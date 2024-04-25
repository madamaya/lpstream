package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple8;

import java.util.function.Supplier;

public class CountAndAvgDistanceGL implements AggregateFunction<NYCInputTupleGL, CountAndAvgDistanceGL.Accumulator, NYCResultTupleGL> {
    private Supplier<ProvenanceAggregateStrategy> strategySupplier;

    public CountAndAvgDistanceGL(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
        this.strategySupplier = strategySupplier;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(strategySupplier);
    }

    @Override
    public Accumulator add(NYCInputTupleGL tuple, Accumulator acc) {
        acc.add(tuple);
        return acc;
    }

    @Override
    public NYCResultTupleGL getResult(Accumulator acc) {
        NYCResultTupleGL out = acc.getAggregatedResult();
        return out;
    }

    @Override
    public Accumulator merge(Accumulator acc1, Accumulator acc2) {
        throw new UnsupportedOperationException("CountAndAVGDistanceGL: merge()");
    }

    public static class Accumulator extends
            GenealogAccumulator<NYCInputTupleGL, NYCResultTupleGL, Accumulator> {

        private Tuple8<Integer, Long, Long, Double, Long, Long, Long, Long> acc = Tuple8.of(-1, -1L, 0L, 0.0, -1L, -1L, -1L, -1L);

        public Accumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(NYCInputTupleGL tuple) {
            acc.f0 = tuple.getVendorId();
            acc.f1 = tuple.getDropoffLocationId();
            acc.f2++;
            acc.f3 += tuple.getTripDistance();
            acc.f4 = Math.max(acc.f4, tuple.getDropoffTime());
            acc.f5 = Math.max(acc.f5, tuple.getDominantOpTime());
            acc.f6 = Math.max(acc.f6, tuple.getKafkaAppendTime());
            acc.f7 = Math.max(acc.f7, tuple.getStimulus());
        }

        @Override
        protected NYCResultTupleGL doGetAggregatedResult() {
            return new NYCResultTupleGL(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4, System.nanoTime() - acc.f5, acc.f6, acc.f7);
        }
    }
}
