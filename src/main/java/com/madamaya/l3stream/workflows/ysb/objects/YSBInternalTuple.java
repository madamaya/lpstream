package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBInternalTuple {
    private String adId;
    private String campaignId;
    private long eventtime;
    private long stimulus;

    public YSBInternalTuple(String adId, String campaignId, long eventtime, long stimulus) {
        this.adId = adId;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
        this.stimulus = stimulus;
    }

    public YSBInternalTuple(String adId, String campaignId, long eventtime) {
        this.adId = adId;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public Long getEventtime() {
        return eventtime;
    }

    public void setEventtime(Long eventtime) {
        this.eventtime = eventtime;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "YSBInternalTuple{" +
                "adId='" + adId + '\'' +
                ", campaignId='" + campaignId + '\'' +
                ", eventtime=" + eventtime +
                '}';
    }
}
