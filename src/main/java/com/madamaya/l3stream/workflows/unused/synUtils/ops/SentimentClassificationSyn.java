package com.madamaya.l3stream.workflows.unused.synUtils.ops;

import com.madamaya.l3stream.workflows.unused.synUtils.objects.___SynInputTuple;
import com.madamaya.l3stream.workflows.unused.synUtils.objects.___SynInternalTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class SentimentClassificationSyn implements MapFunction<___SynInputTuple, ___SynInternalTuple> {
    @Override
    public ___SynInternalTuple map(___SynInputTuple tuple) throws Exception {
        // This includes the comparison between double values.
        // A kind of error may be happened.
        // However, in this case, processing cost is more important than the accuracy of the classification.
        if (tuple.getOverall() >= 4) { // Positive
            return new ___SynInternalTuple(tuple.getAsin(), 1);
        } else if (2 <= tuple.getOverall() && tuple.getOverall() < 4) { // Neutral
            return new ___SynInternalTuple(tuple.getAsin(), 0);
        } else { // Negative
            return new ___SynInternalTuple(tuple.getAsin(), -1);
        }

    }
}
