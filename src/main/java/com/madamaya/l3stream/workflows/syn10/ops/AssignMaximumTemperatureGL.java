package com.madamaya.l3stream.workflows.syn10.ops;

import com.madamaya.l3stream.workflows.syn10.objects.SynTempTestTupleGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class AssignMaximumTemperatureGL extends RichMapFunction<SynTempTupleGL, SynTempTestTupleGL> {

    private transient ValueState<Double> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>(
                "maximumTemp",
                Double.class
        );
        state = getRuntimeContext().getState(desc);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public SynTempTestTupleGL map(SynTempTupleGL synTempTuple) throws Exception {
        Double currentMaxTemp = state.value();
        if (currentMaxTemp == null || synTempTuple.getTemperature() >= currentMaxTemp) {
            currentMaxTemp = synTempTuple.getTemperature();
            state.update(currentMaxTemp);
        }
        SynTempTestTupleGL out = new SynTempTestTupleGL(synTempTuple, currentMaxTemp);
        GenealogMapHelper.INSTANCE.annotateResult(synTempTuple, out);
        return out;
    }
}
