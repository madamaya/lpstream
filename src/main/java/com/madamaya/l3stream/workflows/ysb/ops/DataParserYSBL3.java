package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class DataParserYSBL3 implements MapFunction<KafkaInputString, YSBInputTuple> {
    ObjectMapper om;

    public DataParserYSBL3() {
        this.om = new ObjectMapper();
    }

    @Override
    public YSBInputTuple map(KafkaInputString input) throws Exception {
        JsonNode jNode = om.readTree(input.getStr());
        String adId = jNode.get("ad_id").textValue();
        String eventType = jNode.get("event_type").textValue();
        String campaignId = jNode.get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("event_time").textValue());

        YSBInputTuple tuple = new YSBInputTuple(adId, eventType, campaignId, eventtime);
        return tuple;
    }
}
