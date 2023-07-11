package com.madamaya.l3stream.samples.lr.test1.utils;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyWatermarkGenerator<T extends Tuple3<Integer, Integer, Long>> implements WatermarkGenerator<T> {
    long latestTs = -1;

    @Override
    public void onEvent(T tuple, long l, WatermarkOutput watermarkOutput) {
        latestTs = tuple.f2;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(latestTs));
    }
}
