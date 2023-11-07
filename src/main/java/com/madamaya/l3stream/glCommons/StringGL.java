package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class StringGL implements GenealogTuple {
    private GenealogData gdata;
    private String string;
    private long stimulus;
    private int partitionID;

    public StringGL(String string, long stimulus) {
        this.string = string;
        this.stimulus = stimulus;
    }

    public StringGL(String string, long stimulus, int partitionID) {
        this.string = string;
        this.stimulus = stimulus;
        this.partitionID = partitionID;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
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
        return string.toString();
    }
}
