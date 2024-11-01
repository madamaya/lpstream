package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class InputGL<T> implements GenealogTuple {
    private GenealogData gdata;
    private T value;
    private long stimulus = Long.MIN_VALUE;
    private int partitionID;

    public InputGL(T value, long stimulus) {
        this.value = value;
        this.stimulus = stimulus;
    }

    public InputGL(T value, long stimulus, int partitionID) {
        this.value = value;
        this.stimulus = stimulus;
        this.partitionID = partitionID;
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        gdata.init(genealogTupleType);
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
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
        return value.toString();
    }
}
