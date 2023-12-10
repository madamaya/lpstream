package com.madamaya.l3stream.workflows.nyc.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

import java.text.SimpleDateFormat;

public class NYCInputTupleGL extends NYCInputTuple implements GenealogTuple {
    private GenealogData gdata;

    public NYCInputTupleGL(String line, SimpleDateFormat sdf) {
        super(line, sdf);
    }

    public NYCInputTupleGL(String line, long stimulus, SimpleDateFormat sdf) {
        super(line, stimulus, sdf);
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
        return super.getDropoffTime();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setDropoffTime(timestamp);
    }
}
