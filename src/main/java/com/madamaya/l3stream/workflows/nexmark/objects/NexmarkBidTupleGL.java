package com.madamaya.l3stream.workflows.nexmark.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class NexmarkBidTupleGL extends NexmarkBidTuple implements GenealogTuple {
    private GenealogData gdata;

    public NexmarkBidTupleGL(int eventType) {
        super(eventType);
    }

    public NexmarkBidTupleGL(int eventType, int auctionId, int bidder, long price, String channel, String url, long dateTime, String extra, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        super(eventType, auctionId, bidder, price, channel, url, dateTime, extra, dominantOpTime, kafkaAppendTime, stimulus);
    }

    public NexmarkBidTupleGL(NexmarkBidTupleGL tuple) {
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
