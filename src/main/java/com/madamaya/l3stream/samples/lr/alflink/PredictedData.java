package com.madamaya.l3stream.samples.lr.alflink;

public class PredictedData {
    private long stimulus;
    private String productId;
    private boolean isPositive;

    public PredictedData(String productId, boolean isPositive) {
        this.productId = productId;
        this.isPositive = isPositive;
    }

    public long getStimulus() {
        return stimulus;
    }

    public String getProductId() {
        return productId;
    }

    public boolean isPositive() {
        return isPositive;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setPositive(boolean positive) {
        isPositive = positive;
    }

    @Override
    public String toString() {
        return "PredictedData{" +
                "productId='" + productId + '\'' +
                ", isPositive=" + isPositive +
                '}';
    }
}
