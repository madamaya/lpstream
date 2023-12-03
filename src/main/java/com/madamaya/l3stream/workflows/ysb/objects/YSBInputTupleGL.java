package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

import java.util.List;

public class YSBInputTupleGL extends YSBInputTuple implements GenealogTuple {
    private GenealogData gdata;

    public YSBInputTupleGL(String adId, String eventType, String campaignId, long eventtime) {
        super(adId, eventType, campaignId, eventtime);
    }

    public YSBInputTupleGL(String adId, String eventType, String campaignId, long eventtime, long stimulus) {
        super(adId, eventType, campaignId, eventtime, stimulus);
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
