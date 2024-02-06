package com.madamaya.l3stream.workflows.unused.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinNexL31 extends ProcessJoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple> {
    @Override
    public void processElement(NexmarkAuctionTuple auctionTuple, NexmarkBidTuple bidTuple, ProcessJoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple>.Context context, Collector<NexmarkJoinedTuple> collector) throws Exception {
        collector.collect(
                new NexmarkJoinedTuple(
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
                )
        );
    }
}
