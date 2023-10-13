package com.madamaya.l3stream.workflows.debug.util;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;

public class WG implements WatermarkStrategy<Tuple3<Integer, Integer, Integer>> {
    @Override
    public TimestampAssigner<Tuple3<Integer, Integer, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public long extractTimestamp(Tuple3<Integer, Integer, Integer> t, long l) {
                return t.f2;
            }
        };
    }

    @Override
    public WatermarkGenerator<Tuple3<Integer, Integer, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void onEvent(Tuple3<Integer, Integer, Integer> t, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(t.f2 - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
