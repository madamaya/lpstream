package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBInputTuple {
    // {"user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address", "campaign_id"}
    private String adId;
    private String adType;
    private String compaignId;
    private Long eventtime;

    public YSBInputTuple(String adId, String adType, String compaignId, Long eventtime) {
        this.adId = adId;
        this.adType = adType;
        this.compaignId = compaignId;
        this.eventtime = eventtime;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getAdType() {
        return adType;
    }

    public void setAdType(String adType) {
        this.adType = adType;
    }

    public String getCompaignId() {
        return compaignId;
    }

    public void setCompaignId(String compaignId) {
        this.compaignId = compaignId;
    }

    public Long getEventtime() {
        return eventtime;
    }

    public void setEventtime(Long eventtime) {
        this.eventtime = eventtime;
    }

    @Override
    public String toString() {
        return "YSBInputTuple{" +
                "adId='" + adId + '\'' +
                ", adType='" + adType + '\'' +
                ", compaignId='" + compaignId + '\'' +
                ", eventtime=" + eventtime +
                '}';
    }
}
