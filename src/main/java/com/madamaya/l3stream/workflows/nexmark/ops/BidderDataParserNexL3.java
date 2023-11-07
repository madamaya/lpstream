package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class BidderDataParserNexL3 implements MapFunction<KafkaInputString, NexmarkBidTuple> {
    ObjectMapper om;

    public BidderDataParserNexL3() {
        this.om = new ObjectMapper();
    }
    /*
     Sample Input:
   {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
    // bid: auction, bidder, price, channel, url, dateTime, extra
    */
    @Override
    public NexmarkBidTuple map(KafkaInputString input) throws Exception {
        JsonNode jsonNodes = om.readTree(input.getStr());
        int eventType = jsonNodes.get("event_type").asInt();

        // eventType is auction
        if (eventType == 2) {
            JsonNode jnode = jsonNodes.get("bid");
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
