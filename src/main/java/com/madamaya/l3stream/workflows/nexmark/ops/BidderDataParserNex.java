package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class BidderDataParserNex implements MapFunction<ObjectNode, NexmarkBidTuple> {
    /*
     Sample Input:
   {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
    // bid: auction, bidder, price, channel, url, dateTime, extra
    */
    @Override
    public NexmarkBidTuple map(ObjectNode jsonNodes) throws Exception {
        int eventType = jsonNodes.get("value").get("event_type").asInt();

        // eventType is auction
        if (eventType == 2) {
            JsonNode jnode = jsonNodes.get("value").get("bid");
            int auctionId = jnode.get("auction").asInt();
            int bidder = jnode.get("bidder").asInt();
            long price = jnode.get("price").asLong();
            String channel = jnode.get("channel").asText();
            String url = jnode.get("url").asText();
            long dateTime = NexmarkInputTuple.convertDateStrToLong(jnode.get("dateTime").asText());
            String extra = jnode.get("extra").asText();

            return new NexmarkBidTuple(eventType, auctionId, bidder, price, channel, url, dateTime, extra);
        } else {
            return new NexmarkBidTuple(eventType);
        }
    }
}
