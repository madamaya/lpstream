package com.madamaya.l3stream.workflows.nexmark.objects;

import io.palyvos.provenance.genealog.GenealogData;
import io.palyvos.provenance.genealog.GenealogTuple;
import io.palyvos.provenance.genealog.GenealogTupleType;

public class NexmarkJoinedTupleGL extends NexmarkJoinedTuple implements GenealogTuple {
    private GenealogData gdata;

    public NexmarkJoinedTupleGL(int auctionId, int bidder, long price, String channel, String url, long bid_dateTime, String bid_extra, String itemName, String desc, int initBid, int reserve, long auction_dateTime, long expires, int seller, int category, String auction_extra) {
        super(auctionId, bidder, price, channel, url, bid_dateTime, bid_extra, itemName, desc, initBid, reserve, auction_dateTime, expires, seller, category, auction_extra);
    }

    public NexmarkJoinedTupleGL(int auctionId, int bidder, long price, String channel, String url, long bid_dateTime, String bid_extra, String itemName, String desc, int initBid, int reserve, long auction_dateTime, long expires, int seller, int category, String auction_extra, long stimulus) {
        super(auctionId, bidder, price, channel, url, bid_dateTime, bid_extra, itemName, desc, initBid, reserve, auction_dateTime, expires, seller, category, auction_extra, stimulus);
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
        return super.getAuction_dateTime();
    }

    @Override
    public void setTimestamp(long timestamp) {
        super.setAuction_dateTime(timestamp);
    }
}
