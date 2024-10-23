package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBInternalTuple {
    private String adId;
    private String campaignId;
    private long eventtime;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public YSBInternalTuple(String adId, String campaignId, long eventtime, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        this.adId = adId;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public YSBInternalTuple(String adId, String campaignId, long eventtime) {
        this.adId = adId;
        this.campaignId = campaignId;
        this.eventtime = eventtime;
    }

    public YSBInternalTuple(YSBInternalTuple tuple) {
        this.adId = tuple.getAdId();
        this.campaignId = tuple.getCampaignId();
        this.eventtime = tuple.getEventtime();
        this.dominantOpTime = tuple.getDominantOpTime();
        this.kafkaAppendTime = tuple.getKafkaAppendTime();
        this.stimulus = tuple.getStimulus();
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
        return "YSBInternalTuple{" +
                "adId='" + adId + '\'' +
                ", campaignId='" + campaignId + '\'' +
                ", eventtime=" + eventtime +
                '}';
    }
}
