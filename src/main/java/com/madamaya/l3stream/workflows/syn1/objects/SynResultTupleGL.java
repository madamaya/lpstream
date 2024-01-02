package com.madamaya.l3stream.workflows.syn1.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class SynResultTupleGL extends SynResultTuple implements GenealogTuple {
    private GenealogData gdata;

    public SynResultTupleGL(int machineId, double avgTemp, long timestamp, long stimulus) {
        super(machineId, avgTemp, timestamp, stimulus);
    }

    public SynResultTupleGL(int machineId, double avgTemp, long timestamp) {
        super(machineId, avgTemp, timestamp);
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        this.gdata.init(genealogTupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }
}
