package com.madamaya.l3stream.workflows.ysb.objects;

public class YSBResultTuple {
    private String campaignId;
    private long count;
    private long ts;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public YSBResultTuple(String campaignId, long count, long ts, long kafkaAppendTime, long stimulus) {
        this.campaignId = campaignId;
        this.count = count;
        this.ts = ts;
        this.kafkaAppendTime = kafkaAppendTime;
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
        return "YSBResultTuple{" +
                "campaignId='" + campaignId + '\'' +
                ", count=" + count +
                ", ts=" + ts +
                '}';
    }
}
