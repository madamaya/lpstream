package com.madamaya.l3stream.workflows.lr.ops;

import com.madamaya.l3stream.glCommons.StringGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class DataParserLRGL implements MapFunction<StringGL, LinearRoadInputTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public LinearRoadInputTupleGL map(StringGL input) throws Exception {
        //ObjectNode jNode = jNodeGL.getObjectNode();

        //String line = jNode.get("value").textValue();
        String inputStr = input.getString();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();
        String[] elements = delimiter.split(line);
        LinearRoadInputTupleGL out = new LinearRoadInputTupleGL(
                Integer.valueOf(elements[0]),
                Long.valueOf(elements[1]),
                Integer.valueOf(elements[2]),
                Integer.valueOf(elements[3]),
                Integer.valueOf(elements[4]),
                Integer.valueOf(elements[5]),
                Integer.valueOf(elements[6]),
                Integer.valueOf(elements[7]),
                Integer.valueOf(elements[8]),
                input.getDominantOpTime(),
                input.getKafkaAppandTime(),
                input.getStimulus()
        );
        out.setKey(String.valueOf(out.getVid()));
        //out.setTimestamp(System.currentTimeMillis());
        //out.setPartitionID(input.getPartitionID());

        GenealogMapHelper.INSTANCE.annotateResult(input, out);
        return out;
    }
}
