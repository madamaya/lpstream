package com.madamaya.l3stream.workflows.joinTest;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class WatermarkStrategy2 implements WatermarkStrategy<Tuple3<Integer, Integer, Long>> {
    @Override
    public WatermarkGenerator<Tuple3<Integer, Integer, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Tuple3<Integer, Integer, Long>>() {
            @Override
            public void onEvent(Tuple3<Integer, Integer, Long> tuple3, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple3.f2 - 1000));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }

    @Override
    public TimestampAssigner<Tuple3<Integer, Integer, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<Tuple3<Integer, Integer, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<Integer, Integer, Long> tuple3, long l) {
                return tuple3.f2;
            }
        };
    }
}
