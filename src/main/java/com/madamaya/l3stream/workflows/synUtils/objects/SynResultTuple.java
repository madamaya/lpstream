package com.madamaya.l3stream.workflows.synUtils.objects;

public class SynInternalResult {
    private String asin;
    private long count;

    public SynInternalResult(String asin, long count) {
        this.asin = asin;
        this.count = count;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "SynInternalResult{" +
                "asin='" + asin + '\'' +
                ", count=" + count +
                '}';
    }
}
