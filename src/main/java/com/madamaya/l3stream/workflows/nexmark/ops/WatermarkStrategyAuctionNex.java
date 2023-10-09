package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyAuctionNex implements WatermarkStrategy<NexmarkAuctionTuple> {

    @Override
    public TimestampAssigner<NexmarkAuctionTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NexmarkAuctionTuple>() {
            @Override
            public long extractTimestamp(NexmarkAuctionTuple tuple, long l) {
                return tuple.getDateTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NexmarkAuctionTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NexmarkAuctionTuple>() {
            @Override
            public void onEvent(NexmarkAuctionTuple tuple, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(tuple.getDateTime() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
