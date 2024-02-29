package com.madamaya.l3stream.workflows.utils.ops;

import com.madamaya.l3stream.workflows.utils.objects.MedianCalc;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Aggregation implements AggregateFunction<Tuple3<Integer, Long, Long>, Tuple2<Long, MedianCalc>, Tuple2<Long, Double>> {
    @Override
    public Tuple2<Long, MedianCalc> createAccumulator() {
        return Tuple2.of(0L, new MedianCalc());
    }

    @Override
    public Tuple2<Long, MedianCalc> add(Tuple3<Integer, Long, Long> tuple, Tuple2<Long, MedianCalc> acc) {
        acc.f0 = Math.max(acc.f0, tuple.f2);
        acc.f1.append(tuple.f2 - tuple.f1);
        return acc;
    }

    @Override
    public Tuple2<Long, Double> getResult(Tuple2<Long, MedianCalc> acc) {
        return Tuple2.of(acc.f0, acc.f1.getMedian());
    }

    @Override
    public Tuple2<Long, MedianCalc> merge(Tuple2<Long, MedianCalc> acc1, Tuple2<Long, MedianCalc> acc2) {
        throw new UnsupportedOperationException();
    }
}
