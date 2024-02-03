package com.madamaya.l3stream.workflows.nexmark3.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTupleGL;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinNexGL3 implements JoinFunction<NexmarkAuctionTupleGL, NexmarkBidTupleGL, NexmarkJoinedTupleGL> {
    @Override
    public NexmarkJoinedTupleGL join(NexmarkAuctionTupleGL auctionTuple, NexmarkBidTupleGL bidTuple) throws Exception {
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
                Math.max(bidTuple.getKafkaAppendTime(), auctionTuple.getKafkaAppendTime()),
                Math.max(bidTuple.getStimulus(), auctionTuple.getStimulus())
        );
        GenealogJoinHelper.INSTANCE.annotateResult(auctionTuple, bidTuple, out);

        return out;
    }
}
