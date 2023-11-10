package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;

import java.util.*;

public class WatermarkStrategyLRGL implements WatermarkStrategy<LinearRoadInputTupleGL> {
    @Override
    public TimestampAssigner<LinearRoadInputTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<LinearRoadInputTupleGL>() {
            @Override
            public long extractTimestamp(LinearRoadInputTupleGL linearRoadInputTuple, long l) {
                return Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
            }
        };
    }

    @Override
    public WatermarkGenerator<LinearRoadInputTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<LinearRoadInputTupleGL>() {
            long latest = 0;
            @Override
            public void onEvent(LinearRoadInputTupleGL linearRoadInputTuple, long l, WatermarkOutput watermarkOutput) {
                long ts = Time.seconds(linearRoadInputTuple.getTimestamp()).toMilliseconds();
                if (ts > latest) {
                    watermarkOutput.emitWatermark(new Watermark(latest - 1));
                    latest = ts;
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
