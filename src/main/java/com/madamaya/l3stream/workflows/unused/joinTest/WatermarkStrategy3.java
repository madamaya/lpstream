package com.madamaya.l3stream.workflows.unused.joinTest;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class WatermarkStrategy3 implements WatermarkStrategy<Tuple4<Integer, Integer, Integer, Integer>> {
    @Override
    public WatermarkGenerator<Tuple4<Integer, Integer, Integer, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Tuple4<Integer, Integer, Integer, Integer>>() {
            @Override
            public void onEvent(Tuple4<Integer, Integer, Integer, Integer> tuple4, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple4.f2 - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }

    @Override
    public TimestampAssigner<Tuple4<Integer, Integer, Integer, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<Tuple4<Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractTimestamp(Tuple4<Integer, Integer, Integer, Integer> tuple3, long l) {
                return tuple3.f2;
            }
        };
    }
}
