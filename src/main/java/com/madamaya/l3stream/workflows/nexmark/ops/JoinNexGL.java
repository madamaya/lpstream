package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.*;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinNexGL extends ProcessJoinFunction<NexmarkAuctionTupleGL, NexmarkBidTupleGL, NexmarkJoinedTupleGL> {
    @Override
    public void processElement(NexmarkAuctionTupleGL auctionTuple, NexmarkBidTupleGL bidTuple, ProcessJoinFunction<NexmarkAuctionTupleGL, NexmarkBidTupleGL, NexmarkJoinedTupleGL>.Context context, Collector<NexmarkJoinedTupleGL> collector) throws Exception {
        NexmarkJoinedTupleGL out = new NexmarkJoinedTupleGL(
                bidTuple.getAuctionId(),
                bidTuple.getBidder(),
                bidTuple.getPrice(),
                bidTuple.getChannel(),
                bidTuple.getUrl(),
                bidTuple.getDateTime(),
                bidTuple.getExtra(),
                auctionTuple.getItemName(),
                auctionTuple.getDesc(),
                auctionTuple.getInitBid(),
                auctionTuple.getReserve(),
                auctionTuple.getDateTime(),
                auctionTuple.getExpires(),
                auctionTuple.getSeller(),
                auctionTuple.getCategory(),
                auctionTuple.getExtra(),
                Math.max(bidTuple.getStimulus(), auctionTuple.getStimulus())
        );
        GenealogJoinHelper.INSTANCE.annotateResult(auctionTuple, bidTuple, out);

        collector.collect(out);
    }
}
