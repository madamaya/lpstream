package com.madamaya.l3stream.workflows.nyc.objects;

public class NYCResultTuple {
    private int vendorId;
    private long dropoffLocationId;
    private long count;
    private double avgDistance;
    private long ts;

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

    @Override
    public String toString() {
        return "NYCResultTuple{" +
                "vendorId=" + vendorId +
                ", dropoffLocationId=" + dropoffLocationId +
                ", count=" + count +
                ", avgDistance=" + avgDistance +
                ", ts=" + ts +
                '}';
    }
}
