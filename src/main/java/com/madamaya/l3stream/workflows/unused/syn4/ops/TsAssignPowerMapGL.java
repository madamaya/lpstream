package com.madamaya.l3stream.workflows.unused.syn4.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignPowerMapGL implements MapFunction<SynPowerTupleGL, SynPowerTupleGL> {

    @Override
    public SynPowerTupleGL map(SynPowerTupleGL synPowerTupleGL) throws Exception {
        SynPowerTupleGL out = new SynPowerTupleGL(synPowerTupleGL);
        GenealogMapHelper.INSTANCE.annotateResult(synPowerTupleGL, out);
        out.setStimulus(System.nanoTime());
        return out;
    }
}
