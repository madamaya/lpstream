package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;

public class DataParserNYCL3 implements MapFunction<KafkaInputString, NYCInputTuple> {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    @Override
    public NYCInputTuple map(KafkaInputString input) throws Exception {
        /* Column list
        ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID',
       'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
       'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
       'tolls_amount', 'improvement_surcharge', 'total_amount',
       'congestion_surcharge', 'airport_fee'] */

        String inputStr = input.getStr();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();
        NYCInputTuple tuple = new NYCInputTuple(line, sdf);
        return tuple;
    }
}
