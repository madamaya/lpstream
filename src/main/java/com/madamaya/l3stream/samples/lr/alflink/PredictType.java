package com.madamaya.l3stream.samples.lr.alflink;

import java.io.Serializable;

public class PredictType implements Serializable {
    private int type;

    public PredictType() {}

    public PredictType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
