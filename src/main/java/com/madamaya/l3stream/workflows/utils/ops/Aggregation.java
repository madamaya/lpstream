package com.madamaya.l3stream.workflows.utils.ops;

import com.madamaya.l3stream.workflows.utils.objects.MedianCalc;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class Aggregation implements AggregateFunction<Tuple3<Integer, Long, Long>, Tuple3<Integer, Long, MedianCalc>, Tuple3<Integer, Long, Double>> {
    @Override
    public Tuple3<Integer, Long, MedianCalc> createAccumulator() {
        return Tuple3.of(1, 0L, new MedianCalc());
    }

    @Override
    public Tuple3<Integer, Long, MedianCalc> add(Tuple3<Integer, Long, Long> tuple, Tuple3<Integer, Long, MedianCalc> acc) {
        acc.f0 = tuple.f0;
        acc.f1 = Math.max(acc.f1, tuple.f2);
        acc.f2.append(tuple.f2 - tuple.f1);
        return acc;
    }

    @Override
    public Tuple3<Integer, Long, Double> getResult(Tuple3<Integer, Long, MedianCalc> acc) {
        return Tuple3.of(acc.f0, acc.f1, acc.f2.getMedian());
    }

    @Override
    public Tuple3<Integer, Long, MedianCalc> merge(Tuple3<Integer, Long, MedianCalc> acc1, Tuple3<Integer, Long, MedianCalc> acc2) {
        throw new UnsupportedOperationException();
    }
}
