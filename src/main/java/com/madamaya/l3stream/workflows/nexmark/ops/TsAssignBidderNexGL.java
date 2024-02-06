package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTuple;
import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkBidTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignBidderNexGL implements MapFunction<NexmarkBidTupleGL, NexmarkBidTupleGL> {
    @Override
    public NexmarkBidTupleGL map(NexmarkBidTupleGL in) throws Exception {
        NexmarkBidTupleGL out = new NexmarkBidTupleGL(in);
        GenealogMapHelper.INSTANCE.annotateResult(in, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
