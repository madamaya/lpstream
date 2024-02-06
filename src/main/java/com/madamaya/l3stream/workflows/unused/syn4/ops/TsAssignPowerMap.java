package com.madamaya.l3stream.workflows.unused.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignPowerMap implements MapFunction<SynPowerTuple, SynPowerTuple> {

    @Override
    public SynPowerTuple map(SynPowerTuple synPowerTuple) throws Exception {
        SynPowerTuple out = new SynPowerTuple(synPowerTuple);
        out.setStimulus(System.nanoTime());
        return out;
    }
}
