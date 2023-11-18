package com.madamaya.l3stream.workflows.nexmark4.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinNexL34 implements JoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple> {
    @Override
    public NexmarkJoinedTuple join(NexmarkAuctionTuple auctionTuple, NexmarkBidTuple bidTuple) throws Exception {
        return new NexmarkJoinedTuple(
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
    }
}
