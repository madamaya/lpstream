package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyYSB implements WatermarkStrategy<YSBInputTuple> {

    @Override
    public TimestampAssigner<YSBInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<YSBInputTuple>() {
            @Override
            public long extractTimestamp(YSBInputTuple tuple, long l) {
                return tuple.getEventtime();
            }
        };
    }

    @Override
    public WatermarkGenerator<YSBInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<YSBInputTuple>() {
            @Override
            public void onEvent(YSBInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getEventtime() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
