package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBInputTuple {
    // {"user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address", "campaign_id"}
    private String adId;
    private String eventType;
    private String campaignId;
    private long eventtime;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public YSBInputTuple(String adId, String eventType, String campaignId, long eventtime, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        this.adId = adId;
        this.eventType = eventType;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public YSBInputTuple(String adId, String eventType, String campaignId, long eventtime) {
        this.adId = adId;
        this.eventType = eventType;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
    }

    public String getAdId() {
        return adId;
    }

    public void setAdId(String adId) {
        this.adId = adId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
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

    public long getDominantOpTime() {
        return dominantOpTime;
    }

    public void setDominantOpTime(long dominantOpTime) {
        this.dominantOpTime = dominantOpTime;
    }

    public long getKafkaAppendTime() {
        return kafkaAppendTime;
    }

    public void setKafkaAppendTime(long kafkaAppendTime) {
        this.kafkaAppendTime = kafkaAppendTime;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "YSBInputTuple{" +
                "adId='" + adId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", campaignId='" + campaignId + '\'' +
                ", eventtime=" + eventtime +
                '}';
    }
}
