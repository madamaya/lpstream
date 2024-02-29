package com.madamaya.l3stream.workflows.utils.ops;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;

public class WStrategy implements WatermarkStrategy<Tuple3<Integer, Long, Long>> {
    @Override
    public WatermarkGenerator<Tuple3<Integer, Long, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<Tuple3<Integer, Long, Long>>() {
            @Override
            public void onEvent(Tuple3<Integer, Long, Long> integerLongTuple2, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(integerLongTuple2.f2 - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
