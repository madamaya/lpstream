package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;

public class ConvertJsonDataWinTh2 extends RichMapFunction<ObjectNode, ReviewInputData> {
    long start;
    long count;
    Params params;

    public ConvertJsonDataWinTh2(Params params) {
        this.params = params;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        start = System.currentTimeMillis();
        count = 0L;
        super.open(parameters);
    }

    @Override
    public ReviewInputData map(ObjectNode value) throws Exception {
        long timestamp = System.currentTimeMillis();

        JsonNode j_proID = value.get("value").get("asin");
        JsonNode j_score =  value.get("value").get("overall");
        JsonNode j_rText = value.get("value").get("reviewText");

        String proID = (j_proID != null) ? j_proID.textValue() : "";
        Double score = (j_score != null) ? j_score.doubleValue() : -1.0;
        String rText = (j_rText != null) ? j_rText.textValue() : "";

        ReviewInputData result = new ReviewInputData(proID, score, rText, timestamp);
        count++;

        result.setStimulus(System.nanoTime());

        return result;
    }

    @Override
    public void close() throws Exception {
        long end = System.currentTimeMillis();
        PrintWriter pw = new PrintWriter(System.getenv("HOME") + "/logs/win_baseline/throughput" + getRuntimeContext().getIndexOfThisSubtask() + "_" + end + "_" + params.getCommentLen() + "_" + params.getSleepTime() + "_" + params.getType() + "_" + params.getWindowSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
