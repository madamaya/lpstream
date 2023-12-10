package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.glCommons.InputGL;
import com.madamaya.l3stream.glCommons.JsonNodeGL;
import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BidderDataParserNexGL implements MapFunction<StringGL, NexmarkBidTupleGL> {
    ObjectMapper om;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public BidderDataParserNexGL() {
        this.om = new ObjectMapper();
    }

    /*
     Sample Input:
   {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
    // bid: auction, bidder, price, channel, url, dateTime, extra
    */
    @Override
    public NexmarkBidTupleGL map(StringGL input) throws Exception {
        JsonNode jsonNodes = om.readTree(input.getString());
        int eventType = jsonNodes.get("event_type").asInt();

        // eventType is auction
        if (eventType == 2) {
            JsonNode jnode = jsonNodes.get("bid");
            int auctionId = jnode.get("auction").asInt();
            int bidder = jnode.get("bidder").asInt();
            long price = jnode.get("price").asLong();
            String channel = jnode.get("channel").asText();
            String url = jnode.get("url").asText();
            long dateTime = convertDateFormat(jnode.get("dateTime").asText());
            String extra = jnode.get("extra").asText();

            // NexmarkBidTupleGL out = new NexmarkBidTupleGL(eventType, auctionId, bidder, price, channel, url, dateTime, extra, input.getStimulus());
            NexmarkBidTupleGL out = new NexmarkBidTupleGL(eventType, auctionId, bidder, price, channel, url, dateTime, extra);
            out.setTfl(new TimestampsForLatency(input.getKafkaAppandTime(), input.getStimulus()));
            //out.setDateTime(System.currentTimeMillis());
            GenealogMapHelper.INSTANCE.annotateResult(input, out);

            return out;
        } else {
            return new NexmarkBidTupleGL(eventType);
        }
    }

    private long convertDateFormat(String dateLine) {
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
