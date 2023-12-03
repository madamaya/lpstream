package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.*;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinNexGL extends ProcessJoinFunction<NexmarkAuctionTupleGL, NexmarkBidTupleGL, NexmarkJoinedTupleGL> {
    @Override
    public void processElement(NexmarkAuctionTupleGL auctionTuple, NexmarkBidTupleGL bidTuple, ProcessJoinFunction<NexmarkAuctionTupleGL, NexmarkBidTupleGL, NexmarkJoinedTupleGL>.Context context, Collector<NexmarkJoinedTupleGL> collector) throws Exception {
        long ts = System.currentTimeMillis();
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
                auctionTuple.getExtra()
        );
        if (bidTuple.getStimulusList().get(0) >= auctionTuple.getStimulusList().get(0)) {
            out.setStimulusList(bidTuple.getStimulusList());
        } else {
            out.setStimulusList(auctionTuple.getStimulusList());
        }
        out.setStimulusList(ts);
        GenealogJoinHelper.INSTANCE.annotateResult(auctionTuple, bidTuple, out);

        collector.collect(out);
    }
}
