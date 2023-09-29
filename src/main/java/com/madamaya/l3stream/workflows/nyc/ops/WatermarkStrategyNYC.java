package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

public class WatermarkStrategyNYC implements WatermarkStrategy<NYCInputTuple> {

    @Override
    public TimestampAssigner<NYCInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NYCInputTuple>() {
            @Override
            public long extractTimestamp(NYCInputTuple tuple, long l) {
                return tuple.getDropoffTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NYCInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NYCInputTuple>() {
            @Override
            public void onEvent(NYCInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getDropoffTime() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
