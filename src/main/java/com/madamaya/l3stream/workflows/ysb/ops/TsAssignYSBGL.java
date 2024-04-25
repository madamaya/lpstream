package com.madamaya.l3stream.workflows.ysb.ops;

import com.madamaya.l3stream.workflows.ysb.objects.YSBInternalTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignYSBGL implements MapFunction<YSBInternalTupleGL, YSBInternalTupleGL> {
    @Override
    public YSBInternalTupleGL map(YSBInternalTupleGL in) throws Exception {
        YSBInternalTupleGL out = new YSBInternalTupleGL(in);
        GenealogMapHelper.INSTANCE.annotateResult(in, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
