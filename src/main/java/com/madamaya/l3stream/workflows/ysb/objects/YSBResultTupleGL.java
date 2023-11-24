package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class YSBResultTupleGL extends YSBResultTuple implements GenealogTuple {
    private GenealogData gdata;

    public YSBResultTupleGL(String campaignId, long count, long ts, long stimulus) {
        super(campaignId, count, ts, stimulus);
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
