package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInputTupleGL;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTuple;
import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class ProjectAttributeYSBGL implements MapFunction<YSBInputTupleGL, YSBInternalTupleGL> {

    @Override
    public YSBInternalTupleGL map(YSBInputTupleGL tuple) throws Exception {
        YSBInternalTupleGL out = new YSBInternalTupleGL(tuple.getAdId(), tuple.getCampaignId(), tuple.getEventtime(), tuple.getStimulus());
        GenealogMapHelper.INSTANCE.annotateResult(tuple, out);

        out.setTimestamp(tuple.getTimestamp());
        out.setStimulus(tuple.getStimulus());

        return out;
    }
}
