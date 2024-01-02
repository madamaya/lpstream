package com.madamaya.l3stream.workflows.synUtils.ops;

import com.madamaya.l3stream.workflows.synUtils.objects.___SynInputTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategySyn implements WatermarkStrategy<___SynInputTuple> {

    @Override
    public TimestampAssigner<___SynInputTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<___SynInputTuple>() {
            @Override
            public long extractTimestamp(___SynInputTuple tuple, long l) {
                return tuple.getTs();
            }
        };
    }

    @Override
    public WatermarkGenerator<___SynInputTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<___SynInputTuple>() {
            @Override
            public void onEvent(___SynInputTuple tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getTs() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
