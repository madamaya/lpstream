package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignLRGL implements MapFunction<LinearRoadInputTupleGL, LinearRoadInputTupleGL> {
    @Override
    public LinearRoadInputTupleGL map(LinearRoadInputTupleGL in) throws Exception {
        LinearRoadInputTupleGL out = new LinearRoadInputTupleGL(in);
        GenealogMapHelper.INSTANCE.annotateResult(in, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
