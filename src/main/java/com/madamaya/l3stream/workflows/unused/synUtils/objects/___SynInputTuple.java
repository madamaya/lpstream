package com.madamaya.l3stream.workflows.unused.synUtils.objects;

public class ___SynInputTuple {
    private String asin;
    private double overall;
    private long ts;

    public ___SynInputTuple(String asin, double overall, long ts) {
        this.asin = asin;
        this.overall = overall;
        this.ts = ts;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public double getOverall() {
        return overall;
    }

    public void setOverall(double overall) {
        this.overall = overall;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "SynInputTuple{" +
                "asin='" + asin + '\'' +
                ", overall=" + overall +
                ", ts=" + ts +
                '}';
    }
}
