package com.madamaya.l3stream.workflows.nyc.objects;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class NYCInputTuple {
    /* Column list
        ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID',
       'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
       'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
       'tolls_amount', 'improvement_surcharge', 'total_amount',
       'congestion_surcharge', 'airport_fee'] */

    private int vendorId;
    private long dropoffTime;
    private double tripDistance;
    private long dropoffLocationId;
    private long stimulus = Long.MAX_VALUE;
    // CNFM
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public NYCInputTuple(int vendorId, long dropoffTime, double tripDistance, long dropoffLocationId, long stimulus) {
        this.vendorId = vendorId;
        this.dropoffTime = dropoffTime;
        this.tripDistance = tripDistance;
        this.dropoffLocationId = dropoffLocationId;
        this.stimulus = stimulus;
    }

    public NYCInputTuple(int vendorId, long dropoffTime, double tripDistance, long dropoffLocationId) {
        this.vendorId = vendorId;
        this.dropoffTime = dropoffTime;
        this.tripDistance = tripDistance;
        this.dropoffLocationId = dropoffLocationId;
    }


    public NYCInputTuple(String line, long stimulus) {
        String[] elements = line.split(",");
        this.vendorId = Integer.parseInt(elements[0]);
        this.dropoffTime = convertDateFormat(elements[2]);
        this.tripDistance = Double.parseDouble(elements[4]);
        this.dropoffLocationId = Long.parseLong(elements[8]);
        this.stimulus = stimulus;
    }

    public NYCInputTuple(String line) {
        String[] elements = line.split(",");
        this.vendorId = Integer.parseInt(elements[0]);
        this.dropoffTime = convertDateFormat(elements[2]);
        this.tripDistance = Double.parseDouble(elements[4]);
        this.dropoffLocationId = Long.parseLong(elements[8]);
    }

    public int getVendorId() {
        return vendorId;
    }

    public void setVendorId(int vendorId) {
        this.vendorId = vendorId;
    }

    public long getDropoffTime() {
        return dropoffTime;
    }

    public void setDropoffTime(long dropoffTime) {
        this.dropoffTime = dropoffTime;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public long getDropoffLocationId() {
        return dropoffLocationId;
    }

    public void setDropoffLocationId(long dropoffLocationId) {
        this.dropoffLocationId = dropoffLocationId;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    @Override
    public String toString() {
        return "NYCInputTuple{" +
                "vendorId=" + vendorId +
                ", dropoffTime=" + dropoffTime +
                ", tripDistance=" + tripDistance +
                ", dropoffLocationId=" + dropoffLocationId +
                '}';
    }

    public static long convertDateFormat(String dateLine) {
        Date date;
        Calendar calendar;
        try {
            date = sdf.parse(dateLine);
            calendar = Calendar.getInstance();
            calendar.setTime(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        return calendar.getTimeInMillis();
    }
}
