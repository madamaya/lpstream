package com.madamaya.l3stream.workflows.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignTempMapGL implements MapFunction<SynTempTupleGL, SynTempTupleGL> {
    @Override
    public SynTempTupleGL map(SynTempTupleGL synTempTupleGL) throws Exception {
        SynTempTupleGL out = new SynTempTupleGL(synTempTupleGL);
        out.setStimulus(System.nanoTime());
        return out;
    }
}
