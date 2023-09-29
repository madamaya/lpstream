package com.madamaya.l3stream.workflows.nyc.utils;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

public class WatermarkStrategyNYC implements WatermarkStrategy<LinearRoadInputTuple> {

    @Override
    public TimestampAssigner<LinearRoadInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTuple>() {
            @Override
            public long extractTimestamp(LinearRoadInputTuple linearRoadInputTuple, long l) {
                return Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<LinearRoadInputTuple>() {
            @Override
            public void onEvent(LinearRoadInputTuple linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
