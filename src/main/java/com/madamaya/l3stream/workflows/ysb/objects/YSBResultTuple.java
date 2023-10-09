package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBResultTuple {
    private String campaignId;
    private long count;
    private long ts;

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

    @Override
    public String toString() {
        return "YSBResultTuple{" +
                "campaignId='" + campaignId + '\'' +
                ", count=" + count +
                ", ts=" + ts +
                '}';
    }
}
