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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class AuctionDataParserNexGL implements MapFunction<StringGL, NexmarkAuctionTupleGL> {
    ObjectMapper om;
    final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
            long dateTime = convertDateFormat(jnode.get("dateTime").asText(), sdf1);
            long expires = convertDateFormat(jnode.get("expires").asText(), sdf2);
            int seller = jnode.get("seller").asInt();
            int category = jnode.get("category").asInt();
            String extra = jnode.get("extra").asText();

            // NexmarkAuctionTupleGL out = new NexmarkAuctionTupleGL(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, input.getStimulus());
            NexmarkAuctionTupleGL out = new NexmarkAuctionTupleGL(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, input.getStimulus());
            // NexmarkAuctionTupleGL out = new NexmarkAuctionTupleGL(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, input.getKafkaAppandTime());
            //out.setDateTime(System.currentTimeMillis());
            GenealogMapHelper.INSTANCE.annotateResult(input, out);

            return out;
        } else {
            return new NexmarkAuctionTupleGL(eventType);
        }
    }

    private long convertDateFormat(String dateLine, SimpleDateFormat sdf) {
        Date date;
        Calendar calendar;
        try {
            date = sdf.parse(dateLine);
            calendar = Calendar.getInstance();
            calendar.setTime(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (NumberFormatException e) {
            System.out.println(sdf);
            System.out.println(dateLine);
            throw new RuntimeException(e);
        }

        return calendar.getTimeInMillis();
    }
}
