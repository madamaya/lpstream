package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignAuctionNexL3 implements MapFunction<NexmarkAuctionTuple, NexmarkAuctionTuple> {
    @Override
    public NexmarkAuctionTuple map(NexmarkAuctionTuple in) throws Exception {
        NexmarkAuctionTuple out = new NexmarkAuctionTuple(in);
        return out;
    }
}
