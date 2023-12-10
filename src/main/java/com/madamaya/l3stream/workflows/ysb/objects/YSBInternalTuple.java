package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

public class YSBInternalTuple {
    private String adId;
    private String campaignId;
    private long eventtime;
    private long stimulus = Long.MAX_VALUE;
    private TimestampsForLatency tfl;

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

    public TimestampsForLatency getTfl() {
        return tfl;
    }

    public void setTfl(TimestampsForLatency tfl) {
        this.tfl = tfl;
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
