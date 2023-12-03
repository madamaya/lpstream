package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class ProjectAttributeYSB implements MapFunction<YSBInputTuple, YSBInternalTuple> {

    @Override
    public YSBInternalTuple map(YSBInputTuple tuple) throws Exception {
        long ts = System.currentTimeMillis();
        YSBInternalTuple output = new YSBInternalTuple(tuple.getAdId(), tuple.getCampaignId(), tuple.getEventtime());
        output.setStimulusList(tuple.getStimulusList());
        output.setStimulusList(ts);
        return output;
    }
}
