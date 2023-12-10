package com.madamaya.l3stream.workflows.ysb.objects;

import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

public class YSBResultTuple {
    private String campaignId;
    private long count;
    private long ts;
    private long stimulus = Long.MAX_VALUE;
    private TimestampsForLatency tfl;

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

    public TimestampsForLatency getTfl() {
        return tfl;
    }

    public void setTfl(TimestampsForLatency tfl) {
        this.tfl = tfl;
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
