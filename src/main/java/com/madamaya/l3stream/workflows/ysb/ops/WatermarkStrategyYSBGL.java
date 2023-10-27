package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyYSBGL implements WatermarkStrategy<YSBInputTupleGL> {

    @Override
    public TimestampAssigner<YSBInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<YSBInputTupleGL>() {
            @Override
            public long extractTimestamp(YSBInputTupleGL tuple, long l) {
                return tuple.getEventtime();
            }
        };
    }

    @Override
    public WatermarkGenerator<YSBInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<YSBInputTupleGL>() {
            @Override
            public void onEvent(YSBInputTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getEventtime() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}