package com.madamaya.l3stream.workflows.nexmark.ops;

import com.madamaya.l3stream.workflows.nexmark.objects.NexmarkAuctionTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignAuctionNexGL implements MapFunction<NexmarkAuctionTupleGL, NexmarkAuctionTupleGL> {
    @Override
    public NexmarkAuctionTupleGL map(NexmarkAuctionTupleGL in) throws Exception {
        NexmarkAuctionTupleGL out = new NexmarkAuctionTupleGL(in);
        GenealogMapHelper.INSTANCE.annotateResult(in, out);
        out.setDominantOpTime(System.nanoTime());
        return out;
    }
}
