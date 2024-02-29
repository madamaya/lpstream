package com.madamaya.l3stream.workflows.utils.ops;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class LatencySink extends RichSinkFunction<Tuple2<Long, Double>> {
    private String filePath;
    private BufferedWriter bw;

    public LatencySink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        File file = new File(filePath);
        String basename = file.getName().split("\\.")[0];
        bw = new BufferedWriter(new FileWriter(file.getParentFile() + "/" + basename + "_" + getRuntimeContext().getIndexOfThisSubtask() + ".log"));
    }

    @Override
    public void invoke(Tuple2<Long, Double> value, Context context) throws Exception {
        bw.write(value.f0 + "," + value.f1 + "\n");
        bw.flush();
    }

    @Override
    public void close() throws Exception {
        bw.flush();
        bw.close();
    }
}
