package com.madamaya.l3stream.glCommons;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

public class InitGdataGL extends RichMapFunction<ObjectNode, ObjectNodeGL> {
    long start;
    long count;
    private ExperimentSettings settings;
    private int sourceID;

    public InitGdataGL(ExperimentSettings settings, int sourceID) {
        this.settings = settings;
        this.sourceID = sourceID;
    }

    public InitGdataGL(ExperimentSettings settings) {
        this.settings = settings;
        this.sourceID = 0;
    }

    @Override
    public ObjectNodeGL map(ObjectNode jsonNodes) throws Exception {
        ObjectNodeGL out = new ObjectNodeGL(jsonNodes, System.nanoTime());
        out.initGenealog(GenealogTupleType.SOURCE);
        count++;
        return out;
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
