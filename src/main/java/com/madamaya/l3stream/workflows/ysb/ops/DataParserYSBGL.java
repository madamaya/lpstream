package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTupleGL;
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

public class DataParserYSBGL extends RichMapFunction<KafkaInputStringGL, YSBInputTupleGL> {
    long start;
    long count;
    ExperimentSettings settings;
    ObjectMapper om;

    public DataParserYSBGL(ExperimentSettings settings) {
        this.settings = settings;
        this.om = new ObjectMapper();
    }

    @Override
    public YSBInputTupleGL map(KafkaInputStringGL input) throws Exception {
        JsonNode jNode = om.readTree(input.getStr());
        String adId = jNode.get("ad_id").textValue();
        String eventType = jNode.get("event_type").textValue();
        String campaignId = jNode.get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("event_time").textValue());
        count++;
        YSBInputTupleGL out = new YSBInputTupleGL(adId, eventType, campaignId, eventtime, input.getDominantOpTime(), input.getKafkaAppandTime(), input.getStimulus());
        GenealogMapHelper.INSTANCE.annotateResult(input, out);
        return out;
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

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
