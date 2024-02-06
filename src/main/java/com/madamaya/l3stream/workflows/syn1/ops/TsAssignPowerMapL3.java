package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignPowerMapL3 implements MapFunction<SynPowerTuple, SynPowerTuple> {

    @Override
    public SynPowerTuple map(SynPowerTuple synPowerTuple) throws Exception {
        SynPowerTuple out = new SynPowerTuple(synPowerTuple);
        return out;
    }
}
