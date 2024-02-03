package com.madamaya.l3stream.workflows.nyc.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class NYCResultTupleGL extends NYCResultTuple implements GenealogTuple {
    private GenealogData gdata;

    public NYCResultTupleGL(int vendorId, long dropoffLocationId, long count, double avgDistance, long ts, long kafkaAppendTime, long stimulus) {
        super(vendorId, dropoffLocationId, count, avgDistance, ts, kafkaAppendTime, stimulus);
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
        return super.getTs();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setTs(timestamp);
    }
}
