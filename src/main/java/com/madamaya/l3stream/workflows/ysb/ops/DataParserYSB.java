package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserYSB implements MapFunction<ObjectNode, YSBInputTuple> {

    @Override
    public YSBInputTuple map(ObjectNode jNode) throws Exception {
        String adId = jNode.get("value").get("ad_id").textValue();
        String eventType = jNode.get("value").get("event_type").textValue();
        String campaignId = jNode.get("value").get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("value").get("event_time").textValue());

        return new YSBInputTuple(adId, eventType, campaignId, eventtime);
    }
}
