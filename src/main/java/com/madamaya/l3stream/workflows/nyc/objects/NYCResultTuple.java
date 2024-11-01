package com.madamaya.l3stream.workflows.nyc.objects;

public class NYCResultTuple {
    private int vendorId;
    private long dropoffLocationId;
    private long count;
    private double avgDistance;
    private long ts;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public NYCResultTuple(int vendorId, long dropoffLocationId, long count, double avgDistance, long ts, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        this.vendorId = vendorId;
        this.dropoffLocationId = dropoffLocationId;
        this.count = count;
        this.avgDistance = avgDistance;
        this.ts = ts;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public NYCResultTuple(int vendorId, long dropoffLocationId, long count, double avgDistance, long ts) {
        this.vendorId = vendorId;
        this.dropoffLocationId = dropoffLocationId;
        this.count = count;
        this.avgDistance = avgDistance;
        this.ts = ts;
    }

    public int getVendorId() {
        return vendorId;
    }

    public void setVendorId(int vendorId) {
        this.vendorId = vendorId;
    }

    public long getDropoffLocationId() {
        return dropoffLocationId;
    }

    public void setDropoffLocationId(long dropoffLocationId) {
        this.dropoffLocationId = dropoffLocationId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getAvgDistance() {
        return avgDistance;
    }

    public void setAvgDistance(double avgDistance) {
        this.avgDistance = avgDistance;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
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
        return "NYCResultTuple{" +
                "vendorId=" + vendorId +
                ", dropoffLocationId=" + dropoffLocationId +
                ", count=" + count +
                ", avgDistance=" + String.format("%.10f", avgDistance) +
                ", ts=" + ts +
                '}';
    }
}
