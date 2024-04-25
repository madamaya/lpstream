package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
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

public class AuctionDataParserNex extends RichMapFunction<KafkaInputString, NexmarkAuctionTuple> {
    /*
     Sample Input:
    {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":""},"bid":null}
    auction: id, itemname, description, initialBid, reserve, dateTime, expires, seller, category, extra
     */
    long start;
    long count;
    ExperimentSettings settings;
    ObjectMapper om;
    final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public AuctionDataParserNex(ExperimentSettings settings) {
        this.settings = settings;
        this.om = new ObjectMapper();
    }

    @Override
    public NexmarkAuctionTuple map(KafkaInputString input) throws Exception {
        JsonNode jsonNodes = om.readTree(input.getStr());
        int eventType = jsonNodes.get("event_type").asInt();
        count++;
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

            NexmarkAuctionTuple tuple = new NexmarkAuctionTuple(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, input.getDominantOpTime(), input.getKafkaAppandTime(), input.getStimulus());
            return tuple;
        } else {
            return new NexmarkAuctionTuple(eventType);
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

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + 0 + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
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
