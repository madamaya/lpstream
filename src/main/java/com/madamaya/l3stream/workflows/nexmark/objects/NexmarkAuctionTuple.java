package com.madamaya.l3stream.workflows.nexmark.objects;

public class NexmarkAuctionTuple extends NexmarkInputTuple {
    /*
     Sample Input:
    {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":""},"bid":null}
    auction: id, itemname, description, initialBid, reserve, dataTime, expires, seller, category, extra
     */
    private int auctionId;
    private String itemName;
    private String desc;
    private int initBid;
    private int reserve;
    private long dateTime;
    private long expires;
    private int seller;
    private int category;
    private String extra;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public NexmarkAuctionTuple(int eventType) {
        super(eventType);
    }

    public NexmarkAuctionTuple(int eventType, int auctionId, String itemName, String desc, int initBid, int reserve, long dateTime, long expires, int seller, int category, String extra, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        super(eventType);
        this.auctionId = auctionId;
        this.itemName = itemName;
        this.desc = desc;
        this.initBid = initBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.extra = extra.replace("\\", "");
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public NexmarkAuctionTuple(int eventType, int auctionId, String itemName, String desc, int initBid, int reserve, long dateTime, long expires, int seller, int category, String extra) {
        super(eventType);
        this.auctionId = auctionId;
        this.itemName = itemName;
        this.desc = desc;
        this.initBid = initBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.extra = extra.replace("\\", "");
    }

    public NexmarkAuctionTuple(NexmarkAuctionTuple tuple) {
        super(tuple.getEventType());
        this.auctionId = tuple.getAuctionId();
        this.itemName = tuple.getItemName();
        this.desc = tuple.getDesc();
        this.initBid = tuple.getInitBid();
        this.reserve = tuple.getReserve();
        this.dateTime = tuple.getDateTime();
        this.expires = tuple.getExpires();
        this.seller = tuple.getSeller();
        this.category = tuple.getCategory();
        this.extra = tuple.getExtra();
        this.dominantOpTime = tuple.getDominantOpTime();
        this.kafkaAppendTime = tuple.getKafkaAppendTime();
        this.stimulus = tuple.getStimulus();
    }

    public int getAuctionId() {
        return auctionId;
    }

    public void setAuctionId(int auctionId) {
        this.auctionId = auctionId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public int getInitBid() {
        return initBid;
    }

    public void setInitBid(int initBid) {
        this.initBid = initBid;
    }

    public int getReserve() {
        return reserve;
    }

    public void setReserve(int reserve) {
        this.reserve = reserve;
    }

    public long getDateTime() {
        return dateTime;
    }

    public void setDateTime(long dateTime) {
        this.dateTime = dateTime;
    }

    public long getExpires() {
        return expires;
    }

    public void setExpires(long expires) {
        this.expires = expires;
    }

    public int getSeller() {
        return seller;
    }

    public void setSeller(int seller) {
        this.seller = seller;
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public long getDominantOpTime() {
        return dominantOpTime;
    }

    public void setDominantOpTime(long dominantOpTime) {
        this.dominantOpTime = dominantOpTime;
    }

    public long getKafkaAppendTime() {
        return kafkaAppendTime;
    }

    public void setKafkaAppendTime(long kafkaAppendTime) {
        this.kafkaAppendTime = kafkaAppendTime;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "NexmarkAuctionTuple{" +
                "auctionId=" + auctionId +
                ", itemName='" + itemName + '\'' +
                ", desc='" + desc + '\'' +
                ", initBid=" + initBid +
                ", reserve=" + reserve +
                ", dateTime=" + dateTime +
                ", expires=" + expires +
                ", seller=" + seller +
                ", category=" + category +
                ", extra='" + extra + '\'' +
                '}';
    }
}
