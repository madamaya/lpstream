package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class InitGdataGL extends RichMapFunction<ObjectNode, ObjectNodeGL> {
    private ExperimentSettings settings;
    private int sourceID;

    public InitGdataGL(ExperimentSettings settings, int sourceID) {
        this.settings = settings;
        this.sourceID = sourceID;
    }

    public InitGdataGL(ExperimentSettings settings) {
        this.settings = settings;
        this.sourceID = 0;
    }

    @Override
    public ObjectNodeGL map(ObjectNode jsonNodes) throws Exception {
        ObjectNodeGL out = new ObjectNodeGL(jsonNodes, System.nanoTime());
        out.initGenealog(GenealogTupleType.SOURCE);
        return out;
    }
}
