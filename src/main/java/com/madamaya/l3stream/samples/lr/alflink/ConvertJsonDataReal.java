package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class ConvertJsonDataReal implements MapFunction<ObjectNode, ReviewInputData> {
    @Override
    public ReviewInputData map(ObjectNode value) throws Exception {
        long stimulus = System.nanoTime();

        JsonNode j_proID = value.get("value").get("asin");
        JsonNode j_score =  value.get("value").get("overall");
        JsonNode j_rText = value.get("value").get("reviewText");

        String proID = (j_proID != null) ? j_proID.textValue() : "";
        Double score = (j_score != null) ? j_score.doubleValue() : -1.0;
        String rText = (j_rText != null) ? j_rText.textValue() : "";

        ReviewInputData result = new ReviewInputData(proID, score, rText);

        result.setStimulus(stimulus);

        return result;
    }
}
