package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.glCommons.InputGL;
import com.madamaya.l3stream.glCommons.JsonNodeGL;
import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class AuctionDataParserNexGL implements MapFunction<StringGL, NexmarkAuctionTupleGL> {
    ObjectMapper om;

    public AuctionDataParserNexGL() {
        this.om = new ObjectMapper();
    }

    /*
     Sample Input:
    {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":""},"bid":null}
    auction: id, itemname, description, initialBid, reserve, dateTime, expires, seller, category, extra
     */
    @Override
    public NexmarkAuctionTupleGL map(StringGL input) throws Exception {
        JsonNode jsonNodes = om.readTree(input.getString());

        int eventType = jsonNodes.get("event_type").asInt();

        // eventType is auction
        // id, itemname, description, initialBid, reserve, dateTime, expires, seller, category, extra
        if (eventType == 1) {
            JsonNode jnode = jsonNodes.get("auction");
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

            NexmarkAuctionTupleGL out = new NexmarkAuctionTupleGL(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, input.getStimulus());
            GenealogMapHelper.INSTANCE.annotateResult(input, out);

            return out;
        } else {
            return new NexmarkAuctionTupleGL(eventType);
        }
    }
}
