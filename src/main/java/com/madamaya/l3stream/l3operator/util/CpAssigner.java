package com.madamaya.l3stream.l3operator.util;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

public class CpAssigner<T> implements MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>>, CheckpointListener {
    private long latestCpId = -1;

    @Override
    public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
        L3StreamTupleContainer<T> ret = new L3StreamTupleContainer<>(value);
        ret.setCheckpointId(latestCpId);
        return ret;
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        latestCpId = l;
    }
}
