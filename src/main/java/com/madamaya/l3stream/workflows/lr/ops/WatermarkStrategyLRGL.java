package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

public class WatermarkStrategyLRGL implements WatermarkStrategy<LinearRoadInputTupleGL> {

    @Override
    public TimestampAssigner<LinearRoadInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTupleGL>() {
            @Override
            public long extractTimestamp(LinearRoadInputTupleGL linearRoadInputTuple, long l) {
                return Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<LinearRoadInputTupleGL>() {
            @Override
            public void onEvent(LinearRoadInputTupleGL linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
