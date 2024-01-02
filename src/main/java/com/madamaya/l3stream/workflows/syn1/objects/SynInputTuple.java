package com.madamaya.l3stream.workflows.syn1.objects;

public class SynInputTuple {
    private int type;

    public SynInputTuple(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "SynInputTuple{" +
                "type=" + type +
                '}';
    }
}
