package com.madamaya.l3stream.workflows.linearroad.noprovenance.utils;

import com.madamaya.l3stream.samples.lr.alflink.ReviewInputData;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

public class LrWatermark implements WatermarkStrategy<LinearRoadInputTuple> {

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
