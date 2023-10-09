package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import org.apache.flink.api.common.eventtime.*;

public class WatermarkStrategyBidNex implements WatermarkStrategy<NexmarkBidTuple> {

    @Override
    public TimestampAssigner<NexmarkBidTuple> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<NexmarkBidTuple>() {
            @Override
            public long extractTimestamp(NexmarkBidTuple tuple, long l) {
                return tuple.getDateTime();
            }
        };
    }

    @Override
    public WatermarkGenerator<NexmarkBidTuple> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<NexmarkBidTuple>() {
            @Override
            public void onEvent(NexmarkBidTuple tuple, long l, WatermarkOutput watermarkOutput) {
                // CNFM: ほんとにこれでいい？
                watermarkOutput.emitWatermark(new Watermark(tuple.getDateTime() - 1));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
