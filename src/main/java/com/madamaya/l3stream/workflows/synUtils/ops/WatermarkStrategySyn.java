package com.madamaya.l3stream.workflows.synUtils.ops;

import com.madamaya.l3stream.workflows.synUtils.objects.SynInputTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategySyn implements WatermarkStrategy<SynInputTuple> {

    @Override
    public TimestampAssigner<SynInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<SynInputTuple>() {
            @Override
            public long extractTimestamp(SynInputTuple tuple, long l) {
                return tuple.getTs();
            }
        };
    }

    @Override
    public WatermarkGenerator<SynInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<SynInputTuple>() {
            @Override
            public void onEvent(SynInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getTs() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
