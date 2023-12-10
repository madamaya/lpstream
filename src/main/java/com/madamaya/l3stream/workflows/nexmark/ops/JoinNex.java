package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkJoinedTuple;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class JoinNex extends ProcessJoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple> {
    @Override
    public void processElement(NexmarkAuctionTuple auctionTuple, NexmarkBidTuple bidTuple, ProcessJoinFunction<NexmarkAuctionTuple, NexmarkBidTuple, NexmarkJoinedTuple>.Context context, Collector<NexmarkJoinedTuple> collector) throws Exception {
        NexmarkJoinedTuple out = new NexmarkJoinedTuple(
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
                auctionTuple.getExtra());
        if (auctionTuple.getTfl().ts2 >= bidTuple.getTfl().ts2) {
            long tmp = Math.max(auctionTuple.getTfl().ts1, bidTuple.getTfl().ts1);
            out.setTfl(auctionTuple.getTfl());
            out.getTfl().setTs1(tmp);
        } else if (auctionTuple.getTfl().ts2 < bidTuple.getTfl().ts2) {
            long tmp = Math.max(auctionTuple.getTfl().ts1, bidTuple.getTfl().ts1);
            out.setTfl(bidTuple.getTfl());
            out.getTfl().setTs1(tmp);
        } else {
            throw new IllegalStateException();
        }
        collector.collect(out);
    }
}
