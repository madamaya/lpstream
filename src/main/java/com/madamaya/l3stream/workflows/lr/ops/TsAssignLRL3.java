package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import org.apache.flink.api.common.functions.MapFunction;

public class TsAssignLRL3 implements MapFunction<LinearRoadInputTuple, LinearRoadInputTuple> {
    @Override
    public LinearRoadInputTuple map(LinearRoadInputTuple in) throws Exception {
        LinearRoadInputTuple out = new LinearRoadInputTuple(in);
        return out;
    }
}
