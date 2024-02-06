package com.madamaya.l3stream.workflows.unused.debug.util;

import io.palyvos.provenance.ananke.functions.ProvenanceTupleContainer;
import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Set;

public class LineageTraverser implements MapFunction<ProvenanceTupleContainer<Tuple3<Integer, Integer, String>>, String> {
    GenealogGraphTraverser ggt;
    public LineageTraverser(ExperimentSettings settings) {
        ggt = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Override
    public String map(ProvenanceTupleContainer<Tuple3<Integer, Integer, String>> t) throws Exception {
        Set ret = ggt.getProvenance(t);
        StringBuilder sb = new StringBuilder();
        sb.append("OUTPUT: ").append(t).append(", LINEAGE: ").append(ret.toString());
        return sb.toString();
    }
}
