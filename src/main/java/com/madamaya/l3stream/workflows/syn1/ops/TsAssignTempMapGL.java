package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignTempMapGL implements MapFunction<SynTempTupleGL, SynTempTupleGL> {
    @Override
    public SynTempTupleGL map(SynTempTupleGL synTempTupleGL) throws Exception {
        SynTempTupleGL out = new SynTempTupleGL(synTempTupleGL);
        GenealogMapHelper.INSTANCE.annotateResult(synTempTupleGL, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
