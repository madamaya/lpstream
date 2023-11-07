package com.madamaya.l3stream.workflows.lr.ops;

import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class DataParserLR extends RichMapFunction<KafkaInputString, LinearRoadInputTuple> {
    private static final Pattern delimiter = Pattern.compile(",");
    long start;
    long count;
    ExperimentSettings settings;

    public DataParserLR(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public LinearRoadInputTuple map(KafkaInputString input) throws Exception {
        String line = input.getStr().trim();
        String[] elements = delimiter.split(line);
        LinearRoadInputTuple tuple = new LinearRoadInputTuple(
                Integer.valueOf(elements[0]),
                Long.valueOf(elements[1]),
                Integer.valueOf(elements[2]),
                Integer.valueOf(elements[3]),
                Integer.valueOf(elements[4]),
                Integer.valueOf(elements[5]),
                Integer.valueOf(elements[6]),
                Integer.valueOf(elements[7]),
                Integer.valueOf(elements[8]),
                input.getStimulus()
        );
        tuple.setKey(String.valueOf(tuple.getVid()));
        // tuple.setPartitionID(input.getPartitionID());
        count++;
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
