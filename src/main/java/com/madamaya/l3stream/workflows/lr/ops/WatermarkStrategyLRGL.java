package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyLRGL implements WatermarkStrategy<LinearRoadInputTupleGL> {
    @Override
    public TimestampAssigner<LinearRoadInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTupleGL>() {
            @Override
            public long extractTimestamp(LinearRoadInputTupleGL linearRoadInputTuple, long l) {
                return linearRoadInputTuple.getTimestamp();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<LinearRoadInputTupleGL>() {
            long latest = 0;
            @Override
            public void onEvent(LinearRoadInputTupleGL linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                long ts = linearRoadInputTuple.getTimestamp();
                if (ts > latest) {
                    watermarkOutput.emitWatermark(new Watermark(latest));
                    latest = ts;
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
