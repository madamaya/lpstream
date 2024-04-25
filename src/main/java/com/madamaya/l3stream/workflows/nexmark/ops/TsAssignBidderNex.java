package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignBidderNex implements MapFunction<NexmarkBidTuple, NexmarkBidTuple> {
    @Override
    public NexmarkBidTuple map(NexmarkBidTuple in) throws Exception {
        NexmarkBidTuple out = new NexmarkBidTuple(in);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
