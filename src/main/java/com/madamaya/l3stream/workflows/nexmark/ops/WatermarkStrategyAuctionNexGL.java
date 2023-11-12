package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTupleGL;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyAuctionNexGL implements WatermarkStrategy<NexmarkAuctionTupleGL> {

    @Override
    public TimestampAssigner<NexmarkAuctionTupleGL> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NexmarkAuctionTupleGL>() {
            @Override
            public long extractTimestamp(NexmarkAuctionTupleGL tuple, long l) {
                return tuple.getDateTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NexmarkAuctionTupleGL> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NexmarkAuctionTupleGL>() {
            long latest = 0;
            @Override
            public void onEvent(NexmarkAuctionTupleGL tuple, long l, WatermarkOutput watermarkOutput) {
                if (tuple.getDateTime() > latest) {
                    watermarkOutput.emitWatermark(new Watermark(latest));
                    latest = tuple.getDateTime();
                }
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
