package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class DataParserNYC extends RichMapFunction<KafkaInputString, NYCInputTuple> {
    long start;
    long count;
    ExperimentSettings settings;
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public DataParserNYC(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public NYCInputTuple map(KafkaInputString input) throws Exception {
        /* Column list
        ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
       'passenger_count', 'trip_distance', 'RatecodeID',
       'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
       'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
       'tolls_amount', 'improvement_surcharge', 'total_amount',
       'congestion_surcharge', 'airport_fee'] */
        // String line = jNode.get("value").textValue();
        long ts = System.currentTimeMillis();
        count++;
        String inputStr = input.getStr();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();

        // NYCInputTuple tuple = new NYCInputTuple(line, input.getStimulus(), sdf);
        // NYCInputTuple tuple = new NYCInputTuple(line, input.getKafkaAppandTime(), sdf);
        NYCInputTuple tuple = new NYCInputTuple(line, sdf);
        List<Long> stimulusList = new ArrayList<>();
        stimulusList.add(input.getKafkaAppandTime());
        stimulusList.add(input.getStimulus());
        stimulusList.add(ts);
        tuple.setStimulusList(stimulusList);
        //tuple.setDropoffTime(System.currentTimeMillis());
        return tuple;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        start = System.nanoTime();
        count = 0L;
    }

    @Override
    public void close() throws Exception {
        long end = System.nanoTime();

        String dataPath = L3conf.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
        if (Files.notExists(Paths.get(dataPath))) {
            Files.createDirectories(Paths.get(dataPath));
        }

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + getRuntimeContext().getIndexOfThisSubtask() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
