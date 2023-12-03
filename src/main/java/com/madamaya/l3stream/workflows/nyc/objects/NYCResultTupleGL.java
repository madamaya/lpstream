package com.madamaya.l3stream.workflows.nyc.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

import java.util.List;

public class NYCResultTupleGL extends NYCResultTuple implements GenealogTuple {
    private GenealogData gdata;

    public NYCResultTupleGL(int vendorId, long dropoffLocationId, long count, double avgDistance, long ts) {
        super(vendorId, dropoffLocationId, count, avgDistance, ts);
    }

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
        return super.getTs();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setTs(timestamp);
    }

    @Override
    public List<Long> getStimulusList() {
        return super.getStimulusList();
    }

    @Override
    public void setStimulusList(List<Long> stimulusList) {
        super.setStimulusList(stimulusList);
    }

    @Override
    public void setStimulusList(long stimulus) {
        super.setStimulusList(stimulus);
    }
}
