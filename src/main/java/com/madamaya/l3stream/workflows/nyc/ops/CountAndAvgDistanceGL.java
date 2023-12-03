package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

import java.util.List;
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

        // private Tuple6<Integer, Long, Long, Double, Long, Long> acc = Tuple6.of(-1, -1L, 0L, 0.0, -1L, -1L);
        private Tuple7<Integer, Long, Long, Double, Long, Long, List<Long>> acc = Tuple7.of(-1, -1L, 0L, 0.0, -1L, -1L, null);

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
            // acc.f5 = Math.max(acc.f5, tuple.getStimulus());
            if (acc.f6 == null || acc.f6.get(0) < tuple.getStimulusList().get(0)) {
                acc.f5 = System.currentTimeMillis();
                acc.f6 = tuple.getStimulusList();
            }
        }

        @Override
        protected NYCResultTupleGL doGetAggregatedResult() {
            long ts = System.currentTimeMillis();
            NYCResultTupleGL tuple = new NYCResultTupleGL(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4);
            tuple.setStimulusList(acc.f6);
            tuple.setStimulusList(acc.f5);
            tuple.setStimulusList(ts);
            return tuple;
        }
    }
}
