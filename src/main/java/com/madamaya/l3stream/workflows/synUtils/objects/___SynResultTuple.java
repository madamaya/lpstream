package com.madamaya.l3stream.workflows.synUtils.objects;

public class ___SynResultTuple {
    private String asin;
    private int sentiment;
    private long count;

    public ___SynResultTuple(String asin, int sentiment, long count) {
        this.asin = asin;
        this.sentiment = sentiment;
        this.count = count;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "SynResultTuple{" +
                "asin='" + asin + '\'' +
                ", sentiment=" + sentiment +
                ", count=" + count +
                '}';
    }
}
