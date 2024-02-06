package com.madamaya.l3stream.workflows.nexmark.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class NexmarkAuctionTupleGL extends NexmarkAuctionTuple implements GenealogTuple {
    private GenealogData gdata;

    public NexmarkAuctionTupleGL(int eventType) {
        super(eventType);
    }

    public NexmarkAuctionTupleGL(int eventType, int auctionId, String itemName, String desc, int initBid, int reserve, long dateTime, long expires, int seller, int category, String extra, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        super(eventType, auctionId, itemName, desc, initBid, reserve, dateTime, expires, seller, category, extra, dominantOpTime, kafkaAppendTime, stimulus);
    }

    public NexmarkAuctionTupleGL(NexmarkAuctionTupleGL tuple) {
        super(tuple);
    }

    @Override
    public void initGenealog(GenealogTupleType genealogTupleType) {
        this.gdata = new GenealogData();
        this.gdata.init(genealogTupleType);
    }

    @Override
    public GenealogData getGenealogData() {
        return gdata;
    }

    @Override
    public long getTimestamp() {
        return super.getDateTime();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setDateTime(timestamp);
    }
}
