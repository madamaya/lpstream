package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

import java.util.*;

public class WatermarkStrategyLR implements WatermarkStrategy<LinearRoadInputTuple> {
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
            long latest = Long.MIN_VALUE;
            @Override
            public void onEvent(LinearRoadInputTuple linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                if (linearRoadInputTuple.getTimestamp() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(linearRoadInputTuple.getTimestamp() - 1));
                    latest = linearRoadInputTuple.getTimestamp();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
