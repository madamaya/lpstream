package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBResultTuple {
    private String campaignId;
    private long count;

    public YSBResultTuple(String campaignId, long count) {
        this.campaignId = campaignId;
        this.count = count;
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

    @Override
    public String toString() {
        return "YSBResultTuple{" +
                "campaignId='" + campaignId + '\'' +
                ", count=" + count +
                '}';
    }
}
