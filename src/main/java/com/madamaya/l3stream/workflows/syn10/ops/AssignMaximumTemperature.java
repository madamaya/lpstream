package com.madamaya.l3stream.workflows.syn10.ops;

import com.madamaya.l3stream.workflows.syn10.objects.SynTempTestTuple;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class AssignMaximumTemperature extends RichMapFunction<SynTempTuple, SynTempTestTuple> {

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
    public SynTempTestTuple map(SynTempTuple synTempTuple) throws Exception {
        Double currentMaxTemp = state.value();
        if (currentMaxTemp == null || synTempTuple.getTemperature() >= currentMaxTemp) {
            currentMaxTemp = synTempTuple.getTemperature();
            state.update(currentMaxTemp);
        }
        SynTempTestTuple out = new SynTempTestTuple(synTempTuple, currentMaxTemp);
        return out;
    }
}
