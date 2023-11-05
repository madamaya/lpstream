package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyNYCGL implements WatermarkStrategy<NYCInputTupleGL> {

    @Override
    public TimestampAssigner<NYCInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NYCInputTupleGL>() {
            @Override
            public long extractTimestamp(NYCInputTupleGL tuple, long l) {
                return tuple.getDropoffTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NYCInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NYCInputTupleGL>() {
            long latest = Long.MIN_VALUE;
            @Override
            public void onEvent(NYCInputTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
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
