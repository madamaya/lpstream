package com.madamaya.l3stream.workflows.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignTempMapL3 implements MapFunction<SynTempTuple, SynTempTuple> {
    @Override
    public SynTempTuple map(SynTempTuple synTempTuple) throws Exception {
        SynTempTuple out = new SynTempTuple(synTempTuple);
        return out;
    }
}
