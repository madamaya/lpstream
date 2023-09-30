package com.madamaya.l3stream.workflows.synUtils.objects;

public class SynInputTuple {
    private String asin;
    private double overall;

    public SynInputTuple(String asin, double overall) {
        this.asin = asin;
        this.overall = overall;
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

    @Override
    public String toString() {
        return "SynInputTuple{" +
                "asin='" + asin + '\'' +
                ", overall=" + overall +
                '}';
    }
}
