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
            long latest = 0;
            @Override
            public void onEvent(YSBInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                if (tuple.getEventtime() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(latest - 1));
                    latest = tuple.getEventtime();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
