package com.madamaya.l3stream.workflows.nexmark.objects;

public class NexmarkJoinedTuple {
    private int auctionId;
    private int bidder;
    private long price;
    private String channel;
    private String url;
    private long bid_dateTime;
    private String bid_extra;
    private String itemName;
    private String desc;
    private int initBid;
    private int reserve;
    private long auction_dateTime;
    private long expires;
    private int seller;
    private int category;
    private String auction_extra;
    private long timestamp;
    private long dominantOpTime = Long.MAX_VALUE;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public NexmarkJoinedTuple(int auctionId, int bidder, long price, String channel, String url, long bid_dateTime, String bid_extra, String itemName, String desc, int initBid, int reserve, long auction_dateTime, long expires, int seller, int category, String auction_extra, long timestamp, long dominantOpTime, long kafkaAppendTime, long stimulus) {
        this.auctionId = auctionId;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.bid_dateTime = bid_dateTime;
        this.bid_extra = bid_extra;
        this.itemName = itemName;
        this.desc = desc;
        this.initBid = initBid;
        this.reserve = reserve;
        this.auction_dateTime = auction_dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.auction_extra = auction_extra;
        this.timestamp = timestamp;
        this.dominantOpTime = dominantOpTime;
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public NexmarkJoinedTuple(int auctionId, int bidder, long price, String channel, String url, long bid_dateTime, String bid_extra, String itemName, String desc, int initBid, int reserve, long auction_dateTime, long expires, int seller, int category, String auction_extra, long timestamp) {
        this.auctionId = auctionId;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.bid_dateTime = bid_dateTime;
        this.bid_extra = bid_extra;
        this.itemName = itemName;
        this.desc = desc;
        this.initBid = initBid;
        this.reserve = reserve;
        this.auction_dateTime = auction_dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
        this.auction_extra = auction_extra;
        this.timestamp = timestamp;
    }

    public int getAuctionId() {
        return auctionId;
    }

    public void setAuctionId(int auctionId) {
        this.auctionId = auctionId;
    }

    public int getBidder() {
        return bidder;
    }

    public void setBidder(int bidder) {
        this.bidder = bidder;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getBid_dateTime() {
        return bid_dateTime;
    }

    public void setBid_dateTime(long bid_dateTime) {
        this.bid_dateTime = bid_dateTime;
    }

    public String getBid_extra() {
        return bid_extra;
    }

    public void setBid_extra(String bid_extra) {
        this.bid_extra = bid_extra;
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

    public long getAuction_dateTime() {
        return auction_dateTime;
    }

    public void setAuction_dateTime(long auction_dateTime) {
        this.auction_dateTime = auction_dateTime;
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

    public String getAuction_extra() {
        return auction_extra;
    }

    public void setAuction_extra(String auction_extra) {
        this.auction_extra = auction_extra;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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
        return "NexmarkJoinedTuple{" +
                "auctionId=" + auctionId +
                ", bidder=" + bidder +
                ", price=" + price +
                ", channel='" + channel + '\'' +
                ", url='" + url + '\'' +
                ", bid_dateTime=" + bid_dateTime +
                ", bid_extra='" + bid_extra + '\'' +
                ", itemName='" + itemName + '\'' +
                ", desc='" + desc + '\'' +
                ", initBid=" + initBid +
                ", reserve=" + reserve +
                ", auction_dateTime=" + auction_dateTime +
                ", expires=" + expires +
                ", seller=" + seller +
                ", category=" + category +
                ", auction_extra='" + auction_extra + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
