package com.madamaya.l3stream.workflows.nexmark.objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class NexmarkInputTuple {
    /*
     Sample Input:
    {"event_type":1,"person":null,"auction":{"id":1001,"itemName":"pc","description":"gbyf","initialBid":2940,"reserve":4519,"dateTime":"2023-10-03 05:31:34.28","expires":"2023-10-03 05:31:34.292","seller":1010,"category":13,"extra":""},"bid":null}
     */

    private int eventType;

    public NexmarkInputTuple(int eventType) {
        this.eventType = eventType;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "NexmarkInputTuple{" +
                "eventType=" + eventType +
                '}';
    }
}
