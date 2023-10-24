package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class ObjectNodeGL implements GenealogTuple {
    private GenealogData gdata;
    private ObjectNode objectNode;
    private long stimulus;

    public ObjectNodeGL(ObjectNode objectNode, long stimulus) {
        this.objectNode = objectNode;
        this.stimulus = stimulus;
    }

    public ObjectNode getObjectNode() {
        return objectNode;
    }

    public void setObjectNode(ObjectNode objectNode) {
        this.objectNode = objectNode;
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        gdata.init(genealogTupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getStimulus() {
        return stimulus;
    }

    @Override
    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return objectNode.toString();
    }
}
