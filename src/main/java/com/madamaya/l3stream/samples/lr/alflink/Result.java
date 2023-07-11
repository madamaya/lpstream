package com.madamaya.l3stream.samples.lr.alflink;

public class Result {
    private long stimulus;

    private String productId;
    private boolean isPositive;
    private long count;

    public Result(String productId, long count) {
        throw new UnsupportedOperationException();
    }

    public Result(String productId, boolean isPositive, long count) {
        this.productId = productId;
        this.isPositive = isPositive;
        this.count = count;
    }

    public String getKey() {
        return productId + Boolean.toString(isPositive);
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

    public long getCount() {
        return count;
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

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Result{" +
                "productId='" + productId + '\'' +
                ", isPositive=" + isPositive +
                ", count=" + count +
                '}';
    }
}
