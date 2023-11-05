package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import org.apache.flink.api.common.eventtime.*;

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
            long latest = Long.MIN_VALUE;
            @Override
            public void onEvent(NYCInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                if (tuple.getDropoffTime() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(tuple.getDropoffTime() - 1));
                    latest = tuple.getDropoffTime();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
