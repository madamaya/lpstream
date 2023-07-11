package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.api.common.functions.RichMapFunction;

public class ReasonGeneableOperator extends RichMapFunction<ReviewInputData, PredictedData> {

    Params params;
    public ReasonGeneableOperator(Params params) {
        this.params = params;
    }

    @Override
    public PredictedData map(ReviewInputData value) throws Exception {
        if (params.getSleepTime() > 0) {
            Thread.sleep(params.getSleepTime());
        }

        Boolean rst = true;
        // String rsn = "aaaaaaaaaa";

        PredictedData result = new PredictedData(value.getProductId(), rst);

        result.setStimulus(value.getStimulus());

        return result;
    }

}
