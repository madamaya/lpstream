package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.*;
import io.palyvos.provenance.genealog.GenealogJoinHelper;
import org.apache.flink.api.java.tuple.Tuple2;
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

        GenealogJoinHelper.INSTANCE.annotateResult(auctionTuple, bidTuple, out);

        collector.collect(out);
    }
}
