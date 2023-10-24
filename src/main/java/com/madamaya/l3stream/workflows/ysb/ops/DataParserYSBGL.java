package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.glCommons.ObjectNodeGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DataParserYSBGL implements MapFunction<ObjectNodeGL, YSBInputTupleGL> {

    @Override
    public YSBInputTupleGL map(ObjectNodeGL jNodeGL) throws Exception {

        ObjectNode jNode = jNodeGL.getObjectNode();
        String adId = jNode.get("value").get("ad_id").textValue();
        String eventType = jNode.get("value").get("event_type").textValue();
        String campaignId = jNode.get("value").get("campaign_id").textValue();
        long eventtime = Long.parseLong(jNode.get("value").get("event_time").textValue());

        YSBInputTupleGL out = new YSBInputTupleGL(adId, eventType, campaignId, eventtime, jNodeGL.getStimulus());
        GenealogMapHelper.INSTANCE.annotateResult(jNodeGL, out);

        return out;
    }
}
