package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class AuctionDataParserNexL3 implements MapFunction<ObjectNode, NexmarkAuctionTuple> {
    /*
     Sample Input:
    {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":""},"bid":null}
    auction: id, itemname, description, initialBid, reserve, dateTime, expires, seller, category, extra
     */
    @Override
    public NexmarkAuctionTuple map(ObjectNode jsonNodes) throws Exception {
        int eventType = jsonNodes.get("value").get("event_type").asInt();

        // eventType is auction
        // id, itemname, description, initialBid, reserve, dateTime, expires, seller, category, extra
        if (eventType == 1) {
            JsonNode jnode = jsonNodes.get("value").get("auction");
            int auctionId = jnode.get("id").asInt();
            String itemName = jnode.get("itemName").asText();
            String desc = jnode.get("description").asText();
            int initBid = jnode.get("initialBid").asInt();
            int reserve = jnode.get("reserve").asInt();
            long dateTime = NexmarkInputTuple.convertDateStrToLong(jnode.get("dateTime").asText());
            long expires = NexmarkInputTuple.convertDateStrToLong(jnode.get("expires").asText());
            int seller = jnode.get("seller").asInt();
            int category = jnode.get("category").asInt();
            String extra = jnode.get("extra").asText();

            return new NexmarkAuctionTuple(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra);
        } else {
            return new NexmarkAuctionTuple(eventType);
        }
    }
}