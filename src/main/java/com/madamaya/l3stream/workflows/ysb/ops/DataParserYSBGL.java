package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.glCommons.InputGL;
import com.madamaya.l3stream.glCommons.JsonNodeGL;
import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserYSBGL implements MapFunction<StringGL, YSBInputTupleGL> {
    ObjectMapper om;

    public DataParserYSBGL() {
        this.om = new ObjectMapper();
    }

    @Override
    public YSBInputTupleGL map(StringGL input) throws Exception {
        JsonNode jNode = om.readTree(input.getString());
        String adId = jNode.get("ad_id").textValue();
        String eventType = jNode.get("event_type").textValue();
        String campaignId = jNode.get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("event_time").textValue());

        // YSBInputTupleGL out = new YSBInputTupleGL(adId, eventType, campaignId, eventtime, input.getStimulus());
        YSBInputTupleGL out = new YSBInputTupleGL(adId, eventType, campaignId, eventtime, input.getKafkaAppandTime());
        //out.setEventtime(System.currentTimeMillis());
        GenealogMapHelper.INSTANCE.annotateResult(input, out);

        return out;
    }
}
