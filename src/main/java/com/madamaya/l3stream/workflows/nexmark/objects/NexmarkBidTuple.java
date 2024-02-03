package com.madamaya.l3stream.workflows.nexmark.objects;

public class NexmarkBidTuple extends NexmarkInputTuple {
    /*
     Sample Input:
    {"event_type":2,"person":null,"auction":null,"bid":{"auction":1000,"bidder":2001,"price":1809,"channel":"channel-5901","url":"https://www.nexmark.com/wjeq/xkl/llzy/item.htm?query=1&channel_id=1326972928","dateTime":"2023-10-03 05:31:34.28","extra":"[MNM`IxtngkjlwyyghNZI^O[bhpwaiKOK\\JXszmhft]_]UHIKMZIVH^WH\\U`"}}
     */
    // bid: auction, bidder, price, channel, url, dateTime, extra

    private int auctionId;
    private int bidder;
    private long price;
    private String channel;
    private String url;
    private long dateTime;
    private String extra;
    private long kafkaAppendTime = Long.MAX_VALUE;
    private long stimulus = Long.MAX_VALUE;

    public NexmarkBidTuple(int eventType) {
        super(eventType);
    }

    public NexmarkBidTuple(int eventType, int auctionId, int bidder, long price, String channel, String url, long dateTime, String extra, long kafkaAppendTime, long stimulus) {
        super(eventType);
        this.auctionId = auctionId;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.dateTime = dateTime;
        this.extra = extra.replace("\\", "");
        this.kafkaAppendTime = kafkaAppendTime;
        this.stimulus = stimulus;
    }

    public NexmarkBidTuple(int eventType, int auctionId, int bidder, long price, String channel, String url, long dateTime, String extra) {
        super(eventType);
        this.auctionId = auctionId;
        this.bidder = bidder;
        this.price = price;
        this.channel = channel;
        this.url = url;
        this.dateTime = dateTime;
        this.extra = extra.replace("\\", "");
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

    public long getDateTime() {
        return dateTime;
    }

    public void setDateTime(long dateTime) {
        this.dateTime = dateTime;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
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
        return "NexmarkBidTuple{" +
                "auctionId=" + auctionId +
                ", bidder=" + bidder +
                ", price=" + price +
                ", channel='" + channel + '\'' +
                ", url='" + url + '\'' +
                ", dateTime=" + dateTime +
                ", extra='" + extra + '\'' +
                '}';
    }
}
