package com.madamaya.l3stream.workflows.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignPowerMapGL implements MapFunction<SynPowerTupleGL, SynPowerTupleGL> {

    @Override
    public SynPowerTupleGL map(SynPowerTupleGL synPowerTupleGL) throws Exception {
        SynPowerTupleGL out = new SynPowerTupleGL(synPowerTupleGL);
        out.setStimulus(System.nanoTime());
        return null;
    }
}
