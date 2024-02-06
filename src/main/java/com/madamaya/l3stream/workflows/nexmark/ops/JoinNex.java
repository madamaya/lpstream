package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import org.apache.flink.api.common.functions.JoinFunction;

public class JoinNex implements JoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple> {
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
                auctionTuple.getExtra(),
                System.nanoTime() - Math.max(bidTuple.getDominantOpTime(), auctionTuple.getDominantOpTime()),
                Math.max(bidTuple.getKafkaAppendTime(), auctionTuple.getKafkaAppendTime()),
                Math.max(bidTuple.getStimulus(), auctionTuple.getStimulus())
        );
    }
}
