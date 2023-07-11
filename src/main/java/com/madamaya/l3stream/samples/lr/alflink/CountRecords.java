package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class CountRecords implements
        AggregateFunction<PredictedData, Tuple4<String, Boolean, Long, Long>, Result> {

    @Override
    public Tuple4<String, Boolean, Long, Long> createAccumulator() {
        return Tuple4.of("", false, 0L, -1L);
    }

    @Override
    public Tuple4<String, Boolean, Long, Long> add(
            PredictedData value, Tuple4<String, Boolean, Long, Long> accumulator) {
        accumulator.f0 = value.getProductId();
        accumulator.f1 = value.isPositive();
        accumulator.f2++;
        accumulator.f3 = Math.max(accumulator.f3, value.getStimulus());
        return accumulator;
    }

    @Override
    public Result getResult(Tuple4<String, Boolean, Long, Long> accumulator) {
        Result result = new Result(accumulator.f0, accumulator.f1, accumulator.f2);

        result.setStimulus(accumulator.f3);

        return result;
    }

    @Override
    public Tuple4<String, Boolean, Long, Long> merge(
            Tuple4<String, Boolean, Long, Long> a, Tuple4<String, Boolean, Long, Long> b) {
        throw new UnsupportedOperationException("merge");
    }
}
