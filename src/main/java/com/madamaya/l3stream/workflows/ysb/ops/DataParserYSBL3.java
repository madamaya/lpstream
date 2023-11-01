package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserYSBL3 implements MapFunction<L3StreamInput<JsonNode>, YSBInputTuple> {

    @Override
    public YSBInputTuple map(L3StreamInput<JsonNode> input) throws Exception {
        JsonNode jNode = input.getValue();
        String adId = jNode.get("value").get("ad_id").textValue();
        String eventType = jNode.get("value").get("event_type").textValue();
        String campaignId = jNode.get("value").get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("value").get("event_time").textValue());

        return new YSBInputTuple(adId, eventType, campaignId, eventtime);
    }
}
