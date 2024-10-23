package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyTempSyn implements WatermarkStrategy<SynTempTuple> {

    @Override
    public TimestampAssigner<SynTempTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<SynTempTuple>() {
            @Override
            public long extractTimestamp(SynTempTuple tuple, long l) {
                return tuple.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkGenerator<SynTempTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<SynTempTuple>() {
            long latest = 0;
            @Override
            public void onEvent(SynTempTuple tuple, long l, WatermarkOutput watermarkOutput) {
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
