package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyTempSynGL implements WatermarkStrategy<SynTempTupleGL> {

    @Override
    public TimestampAssigner<SynTempTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<SynTempTupleGL>() {
            @Override
            public long extractTimestamp(SynTempTupleGL tuple, long l) {
                return tuple.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkGenerator<SynTempTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<SynTempTupleGL>() {
            long latest = 0;
            @Override
            public void onEvent(SynTempTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
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
