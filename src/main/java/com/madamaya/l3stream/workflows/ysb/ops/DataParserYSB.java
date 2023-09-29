package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.YSB;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserYSB implements MapFunction<ObjectNode, YSBInputTuple> {

    @Override
    public YSBInputTuple map(ObjectNode jNode) throws Exception {
        String adId = jNode.get("ad_id").textValue();
        String adType = jNode.get("ad_type").textValue();
        String campaignId = jNode.get("campaign_id").textValue();
        long eventtime = jNode.get("event_time").longValue();

        return new YSBInputTuple(adId, adType, campaignId, eventtime);
    }
}
