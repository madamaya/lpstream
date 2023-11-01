package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataParserYSB extends RichMapFunction<L3StreamInput<JsonNode>, YSBInputTuple> {
    long start;
    long count;
    ExperimentSettings settings;

    public DataParserYSB(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public YSBInputTuple map(L3StreamInput<JsonNode> input) throws Exception {
        JsonNode jNode = input.getValue();
        String adId = jNode.get("value").get("ad_id").textValue();
        String eventType = jNode.get("value").get("event_type").textValue();
        String campaignId = jNode.get("value").get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("value").get("event_time").textValue());
        count++;
        return new YSBInputTuple(adId, eventType, campaignId, eventtime, input.getStimulus());
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
