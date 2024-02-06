package com.madamaya.l3stream.workflows.unused.stateTest;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.utils.NonLineageCollectorAdapter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class Wrapper<T, O> extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>>
{
    private final RichMapFunction<T, O> delegate;

    public Wrapper(RichMapFunction<T, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.delegate.setRuntimeContext(getRuntimeContext());
        this.delegate.open(parameters);
    }

    @Override
    public L3StreamTupleContainer<O> map(L3StreamTupleContainer<T> value) throws Exception {
        O result = delegate.map(value.tuple());
        L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(result);
        // GenealogMapHelper.INSTANCE.annotateResult(value, genealogResult);
        genealogResult.copyTimes(value);
        return genealogResult;
    }


    @Override
    public void close() throws Exception {
        this.delegate.close();
    }
}