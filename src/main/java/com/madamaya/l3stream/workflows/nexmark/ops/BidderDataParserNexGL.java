package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BidderDataParserNexGL extends RichMapFunction<KafkaInputStringGL, NexmarkBidTupleGL> {
    long start;
    long count;
    ExperimentSettings settings;
    ObjectMapper om;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public BidderDataParserNexGL(ExperimentSettings settings) {
        this.settings = settings;
        this.om = new ObjectMapper();
    }

    /*
     Sample Input:
   {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
    // bid: auction, bidder, price, channel, url, dateTime, extra
    */
    @Override
    public NexmarkBidTupleGL map(KafkaInputStringGL input) throws Exception {
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

            NexmarkBidTupleGL out = new NexmarkBidTupleGL(eventType, auctionId, bidder, price, channel, url, dateTime, extra, input.getDominantOpTime(), input.getKafkaAppandTime(), input.getStimulus());
            GenealogMapHelper.INSTANCE.annotateResult(input, out);
            return out;
        } else {
            NexmarkBidTupleGL out = new NexmarkBidTupleGL(eventType);
            GenealogMapHelper.INSTANCE.annotateResult(input, out);
            return out;
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

        String dataPath = L3Config.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
        if (Files.notExists(Paths.get(dataPath))) {
            Files.createDirectories(Paths.get(dataPath));
        }

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + 1 + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
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
