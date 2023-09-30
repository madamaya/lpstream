package com.madamaya.l3stream.workflows.synUtils.ops;

import com.madamaya.l3stream.workflows.synUtils.objects.SynInputTuple;
import com.madamaya.l3stream.workflows.synUtils.objects.SynInternalTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class SentimentClassificationSyn implements MapFunction<SynInputTuple, SynInternalTuple> {
    @Override
    public SynInternalTuple map(SynInputTuple tuple) throws Exception {
        // This includes the comparison between double values.
        // A kind of error may be happened.
        // However, in this case, processing cost is more important than the accuracy of the classification.
        if (tuple.getOverall() >= 4) { // Positive
            return new SynInternalTuple(tuple.getAsin(), 1);
        } else if (2 <= tuple.getOverall() && tuple.getOverall() < 4) { // Neutral
            return new SynInternalTuple(tuple.getAsin(), 0);
        } else { // Negative
            return new SynInternalTuple(tuple.getAsin(), -1);
        }

    }
}
