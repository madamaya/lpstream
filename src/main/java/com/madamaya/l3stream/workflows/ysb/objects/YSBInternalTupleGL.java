package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class YSBInternalTupleGL extends YSBInternalTuple implements GenealogTuple {
    private GenealogData gdata;

    public YSBInternalTupleGL(String adId, String campaignId, long eventtime, long kafkaAppendTime, long stimulus) {
        super(adId, campaignId, eventtime, kafkaAppendTime, stimulus);
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
        return super.getEventtime();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setEventtime(timestamp);
    }
}
