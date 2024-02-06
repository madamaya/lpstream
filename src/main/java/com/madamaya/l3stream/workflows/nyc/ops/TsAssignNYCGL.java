package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignNYCGL implements MapFunction<NYCInputTupleGL, NYCInputTupleGL> {
    @Override
    public NYCInputTupleGL map(NYCInputTupleGL in) throws Exception {
        NYCInputTupleGL out = new NYCInputTupleGL(in);
        GenealogMapHelper.INSTANCE.annotateResult(in, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
