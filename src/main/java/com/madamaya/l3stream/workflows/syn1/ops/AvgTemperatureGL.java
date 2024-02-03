package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynResultTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.genealog.GenealogAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.function.Supplier;

public class AvgTemperatureGL implements AggregateFunction<SynTempTupleGL, AvgTemperatureGL.Accumulator, SynResultTupleGL> {
    private Supplier<ProvenanceAggregateStrategy> strategySupplier;

    public AvgTemperatureGL(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
        this.strategySupplier = strategySupplier;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(strategySupplier);
    }

    @Override
    public Accumulator add(SynTempTupleGL tuple, Accumulator acc) {
        acc.add(tuple);
        return acc;
    }

    @Override
    public SynResultTupleGL getResult(Accumulator accumulator) {
        SynResultTupleGL out = accumulator.getAggregatedResult();
        return out;
    }

    @Override
    public Accumulator merge(Accumulator accumulator, Accumulator acc1) {
        throw new UnsupportedOperationException();
    }


    public static class Accumulator extends
            GenealogAccumulator<SynTempTupleGL, SynResultTupleGL, Accumulator> {

        private Tuple6<Integer, Double, Long, Long, Long, Long> acc = Tuple6.of(-1, 0.0, 0L, -1L, -1L, -1L);

        public Accumulator(Supplier<ProvenanceAggregateStrategy> strategySupplier) {
            super(strategySupplier);
        }

        @Override
        protected void doAdd(SynTempTupleGL tuple) {
            acc.f0 = tuple.getMachineId();
            acc.f1 = acc.f1 + tuple.getTemperature();
            acc.f2++;
            acc.f3 = Math.max(acc.f3, tuple.getTimestamp());
            acc.f4 = Math.max(acc.f4, tuple.getKafkaAppendTime());
            acc.f5 = Math.max(acc.f5, tuple.getStimulus());
        }

        @Override
        protected SynResultTupleGL doGetAggregatedResult() {
            return new SynResultTupleGL(acc.f0, acc.f1 / acc.f2, acc.f3, acc.f4, acc.f5);
        }
    }
}
