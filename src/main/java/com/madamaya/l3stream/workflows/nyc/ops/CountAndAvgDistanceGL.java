package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCResultTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

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

        private Tuple6<Integer, Long, Long, Double, Long, TimestampsForLatency> acc = Tuple6.of(-1, -1L, 0L, 0.0, -1L, null);

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
            if (acc.f5 == null) {
                acc.f5 = tuple.getTfl();
            } else {
                if (acc.f5.ts2 >= tuple.getTfl().ts2) {
                    acc.f5.setTs1(Math.max(acc.f5.ts1, tuple.getTfl().ts1));
                } else if (acc.f5.ts2 < tuple.getTfl().ts2) {
                    long tmp = Math.max(acc.f5.ts1, tuple.getTfl().ts1);
                    acc.f5 = tuple.getTfl();
                    acc.f5.setTs1(tmp);
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        protected NYCResultTupleGL doGetAggregatedResult() {
            NYCResultTupleGL out = new NYCResultTupleGL(acc.f0, acc.f1, acc.f2, acc.f3 / acc.f2, acc.f4);
            out.setTfl(acc.f5);
            return out;
        }
    }
}
