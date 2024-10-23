package com.madamaya.l3stream.workflows.nyc.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

import java.text.SimpleDateFormat;

public class NYCInputTupleGL extends NYCInputTuple implements GenealogTuple {
    private GenealogData gdata;

    public NYCInputTupleGL(String line, long dominantOpTime, long kafkaAppendTime, long stimulus, SimpleDateFormat sdf) {
        super(line, dominantOpTime, kafkaAppendTime, stimulus, sdf);
    }

    public NYCInputTupleGL(NYCInputTupleGL tuple) {
        super(tuple);
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
