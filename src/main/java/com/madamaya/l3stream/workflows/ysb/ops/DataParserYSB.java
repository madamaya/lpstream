package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataParserYSB extends RichMapFunction<KafkaInputString, YSBInputTuple> {
    long start;
    long count;
    ExperimentSettings settings;
    ObjectMapper om;

    public DataParserYSB(ExperimentSettings settings) {
        this.settings = settings;
        this.om = new ObjectMapper();
    }

    @Override
    public YSBInputTuple map(KafkaInputString input) throws Exception {
        JsonNode jNode = om.readTree(input.getStr());
        String adId = jNode.get("ad_id").textValue();
        String eventType = jNode.get("event_type").textValue();
        String campaignId = jNode.get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("event_time").textValue());
        count++;
        YSBInputTuple tuple = new YSBInputTuple(adId, eventType, campaignId, eventtime, input.getStimulus());
        tuple.setEventtime(System.currentTimeMillis());
        return tuple;
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

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + getRuntimeContext().getIndexOfThisSubtask() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
