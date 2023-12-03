package com.madamaya.l3stream.workflows.ysb.objects;

import java.util.List;

public class YSBResultTuple {
    private String campaignId;
    private long count;
    private long ts;
    private long stimulus = Long.MAX_VALUE;
    private List<Long> stimulusList;

    public YSBResultTuple(String campaignId, long count, long ts, long stimulus) {
        this.campaignId = campaignId;
        this.count = count;
        this.ts = ts;
        this.stimulus = stimulus;
    }

    public YSBResultTuple(String campaignId, long count, long ts) {
        this.campaignId = campaignId;
        this.count = count;
        this.ts = ts;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    public List<Long> getStimulusList() {
        return stimulusList;
    }

    public void setStimulusList(List<Long> stimulusList) {
        this.stimulusList = stimulusList;
    }

    public void setStimulusList(long stimulus) {
        this.stimulusList.add(stimulus);
    }

    @Override
    public String toString() {
        return "YSBResultTuple{" +
                "campaignId='" + campaignId + '\'' +
                ", count=" + count +
                ", ts=" + ts +
                '}';
    }
}
