package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class YSBInternalTupleGL extends YSBInternalTuple implements GenealogTuple {
    private GenealogData gdata;
    private long timestamp;

    public YSBInternalTupleGL(String adId, String campaignId, long eventtime, long stimulus) {
        super(adId, campaignId, eventtime, stimulus);
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
