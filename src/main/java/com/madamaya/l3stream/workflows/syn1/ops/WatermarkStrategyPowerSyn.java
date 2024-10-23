package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyPowerSyn implements WatermarkStrategy<SynPowerTuple> {

    @Override
    public TimestampAssigner<SynPowerTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<SynPowerTuple>() {
            @Override
            public long extractTimestamp(SynPowerTuple tuple, long l) {
                return tuple.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkGenerator<SynPowerTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<SynPowerTuple>() {
            long latest = 0;
            @Override
            public void onEvent(SynPowerTuple tuple, long l, WatermarkOutput watermarkOutput) {
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
