package com.madamaya.l3stream.workflows.nyc.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class NYCResultTupleGL extends NYCResultTuple implements GenealogTuple {
    private GenealogData gdata;
    private long timestamp;

    public NYCResultTupleGL(int vendorId, long dropoffLocationId, long count, double avgDistance, long ts, long stimulus) {
        super(vendorId, dropoffLocationId, count, avgDistance, ts, stimulus);
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

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
