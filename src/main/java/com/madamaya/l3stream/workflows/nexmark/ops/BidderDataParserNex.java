package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkInputTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BidderDataParserNex extends RichMapFunction<KafkaInputString, NexmarkBidTuple> {
    /*
     Sample Input:
   {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
    // bid: auction, bidder, price, channel, url, dateTime, extra
    */
    long start;
    long count;
    ExperimentSettings settings;
    ObjectMapper om;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public BidderDataParserNex(ExperimentSettings settings) {
        this.settings = settings;
        this.om = new ObjectMapper();
    }

    @Override
    public NexmarkBidTuple map(KafkaInputString input) throws Exception {
        JsonNode jsonNodes = om.readTree(input.getStr());
        int eventType = jsonNodes.get("event_type").asInt();
        count++;
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

            // NexmarkBidTuple tuple = new NexmarkBidTuple(eventType, auctionId, bidder, price, channel, url, dateTime, extra, input.getStimulus());
            NexmarkBidTuple tuple = new NexmarkBidTuple(eventType, auctionId, bidder, price, channel, url, dateTime, extra);
            tuple.setTfl(new TimestampsForLatency(input.getKafkaAppandTime(), input.getStimulus()));
            //tuple.setDateTime(System.currentTimeMillis());
            return tuple;
        } else {
            return new NexmarkBidTuple(eventType);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        start = System.nanoTime();
        count = 0L;
    }

    @Override
    public void close() throws Exception {
        long end = System.nanoTime();

        String dataPath = L3conf.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
        if (Files.notExists(Paths.get(dataPath))) {
            Files.createDirectories(Paths.get(dataPath));
        }

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + 1 + "_" + getRuntimeContext().getIndexOfThisSubtask() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
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
