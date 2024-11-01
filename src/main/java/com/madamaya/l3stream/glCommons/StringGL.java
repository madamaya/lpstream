package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class StringGL implements GenealogTuple {
    private GenealogData gdata;
    private String string;
    private long dominantOpTime;
    private long kafkaAppandTime;
    private long stimulus;

    public StringGL(String string, long dominantOpTime, long kafkaAppandTime, long stimulus) {
        this.string = string;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppandTime = kafkaAppandTime;
        this.stimulus = stimulus;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public long getDominantOpTime() {
        return dominantOpTime;
    }

    public void setDominantOpTime(long dominantOpTime) {
        this.dominantOpTime = dominantOpTime;
    }

    public long getKafkaAppandTime() {
        return kafkaAppandTime;
    }

    public void setKafkaAppandTime(long kafkaAppandTime) {
        this.kafkaAppandTime = kafkaAppandTime;
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        gdata.init(genealogTupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }

    @Override
    public long getTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getStimulus() {
        return stimulus;
    }

    @Override
    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "StringGL{" +
                "string='" + string + "'" +
                '}';
    }
}
