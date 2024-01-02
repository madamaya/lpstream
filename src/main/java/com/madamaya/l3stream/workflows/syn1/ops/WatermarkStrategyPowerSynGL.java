package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyPowerSynGL implements WatermarkStrategy<SynPowerTupleGL> {

    @Override
    public TimestampAssigner<SynPowerTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<SynPowerTupleGL>() {
            @Override
            public long extractTimestamp(SynPowerTupleGL tuple, long l) {
                return tuple.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkGenerator<SynPowerTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<SynPowerTupleGL>() {
            long latest = 0;
            @Override
            public void onEvent(SynPowerTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
                if (tuple.getTimestamp() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(latest));
                    latest = tuple.getTimestamp();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
