package com.madamaya.l3stream.samples.lr.test1.utils;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyWatermarkStrategy<T extends Tuple3<Integer, Integer, Long>> implements WatermarkStrategy<T> {

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return ((tuple, l) -> tuple.f2);
    }

    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWatermarkGenerator<T>();
    }
}

