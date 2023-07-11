package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.api.common.eventtime.*;

public class WatermarkGen implements WatermarkStrategy<ReviewInputData> {
    @Override
    public WatermarkGenerator<ReviewInputData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<ReviewInputData>() {
            @Override
            public void onEvent(ReviewInputData reviewInputData, long l, WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(reviewInputData.getReviewTime()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

            }
        };
    }
}
