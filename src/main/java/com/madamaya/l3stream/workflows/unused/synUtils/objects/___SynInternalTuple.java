package com.madamaya.l3stream.workflows.unused.synUtils.objects;

public class ___SynInternalTuple {
    private String asin;
    private int sentiment; // sentiment == 1: Positive, sentiment == 0: Neutral, sentiment == -1: Negative

    public ___SynInternalTuple(String asin, int sentiment) {
        if (!sentimentValidation(sentiment))
            throw new IllegalArgumentException("SynInternalTuple(String asin=" + asin + ", int sentiment=" + sentiment + ");");

        this.asin = asin;
        this.sentiment = sentiment;
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

    private boolean sentimentValidation(int sentiment) {
        return sentiment == 1 || sentiment == 0 || sentiment == -1;
    }

    @Override
    public String toString() {
        return "SynInternalTuple{" +
                "asin='" + asin + '\'' +
                ", sentiment=" + sentiment +
                '}';
    }
}
