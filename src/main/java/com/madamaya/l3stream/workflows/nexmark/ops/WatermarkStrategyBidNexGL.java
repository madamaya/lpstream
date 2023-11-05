package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyBidNexGL implements WatermarkStrategy<NexmarkBidTupleGL> {

    @Override
    public TimestampAssigner<NexmarkBidTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NexmarkBidTupleGL>() {
            @Override
            public long extractTimestamp(NexmarkBidTupleGL tuple, long l) {
                return tuple.getDateTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NexmarkBidTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NexmarkBidTupleGL>() {

            long latest = Long.MIN_VALUE;
            @Override
            public void onEvent(NexmarkBidTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
                if (tuple.getDateTime() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(tuple.getDateTime() - 1));
                    latest = tuple.getDateTime();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
